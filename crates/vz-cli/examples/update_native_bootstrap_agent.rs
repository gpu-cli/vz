//! Maintainer-only update of a new native disk clone. Not a consumer bootstrap path.
#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    use std::os::unix::fs::DirBuilderExt;
    use std::{path::PathBuf, sync::Arc, time::Duration};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use vz::{DiskConfig, MacPlatformConfig, NetworkConfig, Vm, VmConfigBuilder, VmState};
    use vz_linux::{ContainerExecDispatchGate, grpc_client::GrpcAgentClient};
    let args = std::env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    ensure!(
        args.len() == 5,
        "usage: update_native_bootstrap_agent <disk> <hardware> <auxiliary> <new-directory> <new-agent>"
    );
    let [disk, hardware, auxiliary, directory, agent] = args.as_slice() else {
        unreachable!()
    };
    ensure!(
        directory.is_absolute(),
        "absolute new maintainer directory required"
    );
    std::fs::DirBuilder::new().mode(0o700).create(directory)?;
    ensure!(
        std::process::Command::new("/bin/cp")
            .args(["-c"])
            .arg(disk)
            .arg(directory.join("disk.img"))
            .status()?
            .success(),
        "clone failed"
    );
    std::fs::copy(hardware, directory.join("hardware-model"))?;
    std::fs::copy(auxiliary, directory.join("auxiliary-storage"))?;
    std::fs::write(
        directory.join("machine-identifier"),
        vz::install::generate_machine_id_data()?,
    )?;
    let config = VmConfigBuilder::new()
        .boot_macos()
        .cpus(4)
        .memory_mb(8192)
        .disk(DiskConfig {
            id: "system".into(),
            path: directory.join("disk.img"),
            read_only: false,
        })
        .mac_platform(MacPlatformConfig {
            hardware_model_path: directory.join("hardware-model"),
            auxiliary_storage_path: directory.join("auxiliary-storage"),
            machine_identifier_path: directory.join("machine-identifier"),
        })
        .network(NetworkConfig::None)
        .enable_vsock()
        .build()?;
    let vm = Arc::new(Vm::create(config).await?);
    vm.start().await?;
    let result = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut client = loop {
            ensure!(tokio::time::Instant::now() < deadline, "maintainer guest agent unavailable");
            if let Ok(mut c) = GrpcAgentClient::connect_default(Arc::clone(&vm)).await {
                if c.ping().await.is_ok() { break c; }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let encoded = std::process::Command::new("/usr/bin/base64").arg("-i").arg(agent).output()?;
        ensure!(encoded.status.success(), "encode agent failed");
        let encoded = String::from_utf8(encoded.stdout)?.replace(['\r','\n'], "");
        ensure!(encoded.bytes().all(|b| b.is_ascii_alphanumeric() || b"+/=".contains(&b)), "unexpected encoding");
        let init = client.exec_stream("/bin/sh".into(), vec!["-c".into(), "set -eu; umask 077; test ! -e /private/var/tmp/vz-agent-update.b64; : > /private/var/tmp/vz-agent-update.b64".into()], Default::default()).await?.collect().await;
        ensure!(init.exit_code == 0, "staging failed: {init:?}");
        for bytes in encoded.as_bytes().chunks(32 * 1024) {
            let script = format!("printf '%s' '{}' >> /private/var/tmp/vz-agent-update.b64", std::str::from_utf8(bytes)?);
            let result = client.exec_stream("/bin/sh".into(), vec!["-c".into(),script], Default::default()).await?.collect().await;
            ensure!(result.exit_code == 0, "transfer failed: {result:?}");
        }
        let install = client.exec_stream("/bin/sh".into(), vec!["-c".into(), "set -eu; /usr/bin/base64 -D -i /private/var/tmp/vz-agent-update.b64 > /usr/local/bin/vz-guest-agent.new; chmod 755 /usr/local/bin/vz-guest-agent.new; mv /usr/local/bin/vz-guest-agent.new /usr/local/bin/vz-guest-agent; rm /private/var/tmp/vz-agent-update.b64; /usr/bin/shasum -a 256 /usr/local/bin/vz-guest-agent; sync".into()], Default::default()).await?.collect().await;
        ensure!(install.exit_code == 0, "install failed: {install:?}");
        std::fs::write(directory.join("agent-install.json"), serde_json::to_vec_pretty(&install)?)?;
        for request in [serde_json::json!({"type":"unregister","name":"vz-guest-agent","stop":true}), serde_json::json!({"type":"register","service":{"name":"vz-guest-agent","binary":"/usr/local/bin/vz-guest-agent","args":[],"env":[],"keep_alive":true},"start_now":true})] {
            let mut stream = BufReader::new(vm.vsock_connect(7420).await?);
            stream.get_mut().write_all(format!("{request}\n").as_bytes()).await?;
            let mut line = String::new();
            tokio::time::timeout(Duration::from_secs(15), stream.read_line(&mut line)).await??;
            let response: serde_json::Value = serde_json::from_str(&line)?;
            ensure!(response["type"] != "error", "loader update failed: {response}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut client = GrpcAgentClient::connect_default(Arc::clone(&vm)).await?;
        let mut evidence = Vec::new();
        for pty in [None, Some((24,80))] {
            let ticket = client.prepare_machine_exec_request().await?;
            let (stream, _) = client.exec_machine_stream_ready_for_request(ContainerExecDispatchGate::new(tokio::time::Instant::now()+Duration::from_secs(20)), ticket, "/bin/sh".into(), vec!["-c".into(), if pty.is_some() { "test -t 0 && test -t 1 && printf native-pty" } else { "printf native-pipe; exit 23" }.into()], Default::default(), pty).await?;
            let result = tokio::time::timeout(Duration::from_secs(20), stream.collect()).await?;
            ensure!(result.exit_code == if pty.is_some() { 0 } else { 23 }, "supervision failed: {result:?}");
            evidence.push(result);
        }
        let ticket = client.prepare_machine_exec_request().await?;
        let (stream, id) = client.exec_machine_stream_ready_for_request(ContainerExecDispatchGate::new(tokio::time::Instant::now()+Duration::from_secs(20)), ticket.clone(), "/bin/sh".into(), vec!["-c".into(), "/bin/sleep 120 & wait".into()], Default::default(), None).await?;
        let cancelled = tokio::time::timeout(Duration::from_secs(20), client.cancel_exec(id)).await??;
        let reconciled = client.reconcile_exec_request(ticket).await?;
        drop(stream);
        std::fs::write(directory.join("supervised-exec.json"), serde_json::to_vec_pretty(&serde_json::json!({"results":evidence,"cancel_exit_code":cancelled.exit_code,"reconcile_outcome":reconciled.outcome,"scope":"MAINTAINER_AGENT_PREREQUISITE"}))?)?;
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
    std::fs::write(
        directory.join("shutdown.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"stopped":*state.borrow()==VmState::Stopped,"forced":!matches!(stopped, Ok(Ok(()))),"update_passed":result.is_ok()}),
        )?,
    )?;
    result.context("maintainer native agent update")?;
    ensure!(
        matches!(stopped, Ok(Ok(()))),
        "graceful maintainer shutdown failed"
    );
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("native maintainer update requires Apple-silicon macOS")
}
