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
    let probe = activation.exec("/bin/sh".into(),vec!["-c".into(),"set -eu; /usr/bin/sw_vers -productVersion; /usr/bin/sw_vers -buildVersion; /usr/sbin/sysctl -n hw.model; /usr/bin/shasum -a 256 /usr/local/bin/vz-guest-agent".into()],Duration::from_secs(20)).await.map_err(|e|bad(e.to_string()))?;
    let expected = format!(
        "{}\n{}\nVirtualMac2,1\n{}  /usr/local/bin/vz-guest-agent\n",
        pin.release().macos_version,
        pin.release().macos_build,
        pin.release().guest_agent_sha256
    );
    if probe.exit_code != 0 || probe.stdout != expected || !probe.stderr.is_empty() {
        return Err(bad(format!(
            "native version, hardware or guest-agent pin check failed: {probe:?}"
        )));
    }
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
            .map_err(|e| bad(e.to_string()))?;
        if receipt.exit_code != 0 || !receipt.stderr.is_empty() {
            return Err(bad("pinned native toolchain receipt is unavailable".into()));
        }
        let toolchain = ToolchainManifest::from_verified_bytes(
            receipt.stdout.as_bytes(),
            &pin.release().toolchain_sha256,
        )
        .map_err(|e| bad(e.to_string()))?;
        let (script, expected) = toolchain.verification().map_err(|e| bad(e.to_string()))?;
        let observed = activation
            .exec(
                "/bin/sh".into(),
                vec!["-c".into(), script],
                Duration::from_secs(30),
            )
            .await
            .map_err(|e| bad(e.to_string()))?;
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
