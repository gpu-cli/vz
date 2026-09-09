//! Readiness is measured in the exact native VM, never inferred from its image.
use super::*;
use crate::native_macos::artifacts::NativePin;

pub(super) async fn verify(
    activation: &Arc<MachineRuntimeActivation>,
    pin: &NativePin,
    machine: &MachineInstance,
    incarnation: MachineIncarnation,
    deadline: tokio::time::Instant,
    metadata: &RequestMetadata,
) -> Result<MachineActivationEvidence, MachineError> {
    let bad = |e: String| failure(metadata, MachineErrorCode::BackendUnavailable, e);
    pin.validate_current().map_err(|e| bad(e.to_string()))?;
    let lease = activation
        .native_lease()
        .ok_or_else(|| bad("native readiness received a different backend".into()))?;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(bad(
                "native agent readiness exceeded Up deadline; VM retained for Stop".into(),
            ));
        }
        if let Ok(mut client) = lease.client().await {
            if client.ping().await.is_ok() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let started = std::time::Instant::now();
    tracing::info!("verifying native OS and guest-agent identity");
    let probe = activation.exec("/bin/sh".into(),vec!["-c".into(),"set -eu; /usr/bin/sw_vers -productVersion; /usr/bin/sw_vers -buildVersion; /usr/sbin/sysctl -n hw.model; /usr/bin/openssl dgst -sha256 -r /usr/local/bin/vz-guest-agent".into()],Duration::from_secs(20)).await.map_err(|e|bad(format!("native OS/agent probe: {e}")))?;
    let expected = format!(
        "{}\n{}\nVirtualMac2,1\n{} */usr/local/bin/vz-guest-agent\n",
        pin.release().macos_version,
        pin.release().macos_build,
        pin.release().guest_agent_sha256
    );
    if probe.exit_code != 0 || probe.stdout != expected || !probe.stderr.is_empty() {
        return Err(bad(format!(
            "native version, hardware or guest-agent pin check failed: {probe:?}"
        )));
    }
    tracing::info!(
        elapsed_seconds = started.elapsed().as_secs_f64(),
        "native OS and guest-agent identity verified"
    );
    // Only now: the address is applied by the agent, so it is applied to the
    // guest whose agent has just been proved to be the pinned one.
    configure_fabric_ports(activation, lease, metadata).await?;
    if !pin.release().toolchain_sha256.is_empty() {
        use vz_macos_provision::toolchain::{MAX_RECEIPT_BYTES, RECEIPT_PATH, ToolchainManifest};
        let receipt = activation
            .exec(
                "/usr/bin/head".into(),
                vec![
                    "-c".into(),
                    (MAX_RECEIPT_BYTES + 1).to_string(),
                    RECEIPT_PATH.into(),
                ],
                Duration::from_secs(10),
            )
            .await
            .map_err(|e| bad(format!("native toolchain receipt probe: {e}")))?;
        if receipt.exit_code != 0 || !receipt.stderr.is_empty() {
            return Err(bad("pinned native toolchain receipt is unavailable".into()));
        }
        let toolchain = ToolchainManifest::from_verified_bytes(
            receipt.stdout.as_bytes(),
            &pin.release().toolchain_sha256,
        )
        .map_err(|e| bad(e.to_string()))?;
        let (script, expected) = toolchain.verification().map_err(|e| bad(e.to_string()))?;
        let started = std::time::Instant::now();
        tracing::info!("verifying native compiler and SDK identity");
        let observed = activation
            .exec(
                "/bin/sh".into(),
                vec!["-c".into(), script],
                Duration::from_secs(120),
            )
            .await
            .map_err(|e| bad(format!("native compiler/SDK identity probe: {e}")))?;
        tracing::info!(
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "native compiler and SDK probe completed"
        );
        if observed.exit_code != 0 || observed.stdout != expected || !observed.stderr.is_empty() {
            return Err(bad(format!(
                "native Swift/toolchain pin verification failed: {observed:?}"
            )));
        }
    }
    let ticket = activation
        .execution_lease()
        .prepare_machine_exec_request()
        .await
        .map_err(|e| bad(e.to_string()))?;
    let (stream, _) = activation
        .execution_lease()
        .start_machine_exec(
            vz_linux::ContainerExecDispatchGate::new(deadline),
            ticket,
            "/bin/sh".into(),
            vec![
                "-c".into(),
                "test -t 0 && test -t 1 && printf vz-native-pty".into(),
            ],
            Default::default(),
            Some((24, 80)),
        )
        .await
        .map_err(|e| bad(e.to_string()))?;
    // On timeout the original VM remains owned by Up/Stop; dropping this
    // observation is not positive process or VM termination evidence.
    let pty = tokio::time::timeout_at(deadline, stream.collect())
        .await
        .map_err(|_| bad("native PTY readiness timed out; VM retained for Stop".into()))?;
    if pty.exit_code != 0 || pty.stdout != "vz-native-pty" {
        return Err(bad("native PTY readiness failed".into()));
    }
    let capabilities =
        CapabilitySet::new([MachineCapability::PosixExec, MachineCapability::PosixPty]);
    if !machine
        .requested_capabilities
        .unaccounted_by(&capabilities)
        .is_empty()
    {
        return Err(bad("native requested capabilities lack evidence".into()));
    }
    Ok(MachineActivationEvidence {
        schema_version: 1,
        backend: MachineBackend::MacosNative,
        incarnation,
        negotiated_capabilities: capabilities,
        docker_context: None,
        runtime_identity: MachineRuntimeIdentity {
            schema_version: 1,
            opaque_id: serde_json::to_string(activation.runtime_identity())
                .map_err(|e| bad(e.to_string()))?,
        },
    })
}

/// Give this Machine's Environment-network ports the addresses its fabric plan
/// derived, and prove each one landed.
///
/// This is the macOS half of what `linux/initramfs/init` does for a Linux guest.
/// It happens later than the Linux path — after the guest agent answers rather
/// than before anything in the guest runs — because a macOS boot loader takes no
/// kernel arguments this host could write, so the vsock channel the agent serves
/// is the only one that reaches a guest which does not yet have an address.
/// Readiness runs before Ready is published, so a Machine whose ports could not
/// be configured fails Up rather than coming up unaddressed.
///
/// The address itself is unchanged by any of that: it is the one
/// `environment_switch::plan` derived, applied here and never leased, so a
/// Machine that stops and comes back presents the address its switch expects.
///
/// Each port is judged on stdout read back off the interface, not on the exit
/// status of the configuring command. `ifconfig` exiting zero says the request
/// was accepted; only reading the address back off the NIC says the Machine has
/// it.
async fn configure_fabric_ports(
    activation: &Arc<MachineRuntimeActivation>,
    lease: &crate::native_macos::runtime::NativeMacosLease,
    metadata: &RequestMetadata,
) -> Result<(), MachineError> {
    let bad = |e: String| failure(metadata, MachineErrorCode::BackendUnavailable, e);
    for declaration in lease.attachments() {
        let rendered = crate::native_macos::fabric::configure_fabric_port(declaration);
        let started = std::time::Instant::now();
        let applied = activation
            .exec(
                rendered.command.clone(),
                rendered.args.clone(),
                Duration::from_secs(30),
            )
            .await
            .map_err(|e| {
                bad(format!(
                    "native fabric port {} on {}: {e}",
                    declaration.ipv4, declaration.mac
                ))
            })?;
        if applied.exit_code != 0
            || applied.stdout != rendered.expected_stdout
            || !applied.stderr.is_empty()
        {
            return Err(bad(format!(
                "native fabric port {}/{} on {} did not come up holding its derived address: {applied:?}",
                declaration.ipv4, declaration.prefix, declaration.mac
            )));
        }
        tracing::info!(
            network_id = %declaration.network_id,
            mac = %declaration.mac,
            address = %declaration.ipv4,
            prefix = declaration.prefix,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "native Environment-network port configured"
        );
    }
    Ok(())
}
