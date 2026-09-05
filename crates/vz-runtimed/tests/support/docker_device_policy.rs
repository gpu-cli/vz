//! Differential proof of default Docker device-open policy, not privileged mode.
//! Moby docker-v29.7.2 daemon/pkg/oci/defaults.go denies unspecified devices;
//! daemon/oci_linux.go appends DeviceCgroupRules. Pinned youki 0.7 common.rs
//! default_allow_devices grants wildcard mknod and TUN read/write, but does not
//! grant loop-control (character 10:237). Linux 6.12 loop_ctl_fops uses
//! nonseekable_open, with no device mutation or ioctl in this open-only probe.
//! BusyBox 1.37 dd opens its input before count=0 suppresses reads. The original
//! TUN assertion was invalid for the upstream runtime's compatibility defaults;
//! schema 2 names the corrected target and cannot certify that old receipt.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;

use super::{docker_stdout, host_docker};

use super::docker_namespace_values as namespace_values;
#[path = "docker_device_policy_values.rs"]
mod values;

const IMAGE: &str = "vz-endpoint-fixture:local";
const RULE: &str = "c 10:237 rwm";
const INSPECT: &str =
    r#"{"state":{{json .State}},"host_config":{{json .HostConfig}},"id":{{json .Id}}}"#;
const SETUP: &str = r#"set -eu
/bin/busybox mknod /dev/vz-policy-null c 1 3
/bin/busybox mknod /dev/vz-policy-loop-control c 10 237
/bin/busybox stat -c '%F:%t:%T' /dev/vz-policy-null /dev/vz-policy-loop-control
printf baseline >/dev/null
printf baseline >/dev/vz-policy-null
"#;

pub(super) async fn case(socket: &Path, config: &Path, allow_loop_control: bool) -> Result<Value> {
    let directory = tempfile::Builder::new()
        .prefix("device-policy-")
        .tempdir_in(config)?;
    let cid_path = directory.path().join("container.id");
    let cid_argument = cid_path.to_str().context("UTF-8 private cidfile")?;
    let mut args = vec![
        "run",
        "--detach",
        "--network",
        "none",
        "--env",
        "LC_ALL=C",
        "--cidfile",
        cid_argument,
    ];
    if allow_loop_control {
        args.extend(["--device-cgroup-rule", RULE]);
    }
    args.extend([IMAGE, "/bin/busybox", "sleep", "300"]);
    let run = host_docker(socket, config, &args, vec![]).await;
    let mut commands = BTreeMap::new();
    if let Ok(value) = &run {
        commands.insert("run", value.clone());
    }
    let cid_file = std::fs::read_to_string(&cid_path);
    let id = cid_file.as_ref().ok()
        .and_then(|value| namespace_values::container_id(value).ok())
        .or_else(|| run.as_ref().ok().and_then(|value| docker_stdout(value).ok())
            .and_then(|value| namespace_values::container_id(value).ok()))
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("no validated cleanup ID; cleanup cannot be claimed: run={run:?}; cidfile={cid_file:?}; commands={commands:?}"))?;

    let proof = async {
        ensure!(namespace_values::container_id(docker_stdout(&run?)?)? == id, "run/cidfile mismatch");
        let inspected = host_docker(socket, config, &["inspect", "--format", INSPECT, &id], vec![]).await?;
        commands.insert("inspect", inspected.clone());
        let inspection: Value = serde_json::from_str(docker_stdout(&inspected)?)?;
        ensure!(inspection["id"] == id && inspection["state"]["Running"] == true, "wrong or stopped container");
        let host = &inspection["host_config"];
        ensure!(host["Runtime"] == "youki" && host["Privileged"] == false, "non-default runtime/privilege");
        ensure!(host["NetworkMode"] == "none", "unexpected network mode");
        for field in ["CapAdd", "CapDrop", "SecurityOpt", "Devices"] {
            ensure!(host[field].is_null() || host[field].as_array().is_some_and(Vec::is_empty), "unexpected {field} override");
        }
        if allow_loop_control {
            ensure!(host["DeviceCgroupRules"] == json!([RULE]), "missing exact positive-control rule");
        } else {
            ensure!(host["DeviceCgroupRules"].is_null() || host["DeviceCgroupRules"].as_array().is_some_and(Vec::is_empty), "baseline has device-rule override");
        }
        // Preserve both observations before validating either: an exec path may
        // lose seccomp even when the container init was correctly filtered.
        let init_status = host_docker(socket, config, &["exec", &id, "/bin/busybox", "grep", "-E", "^(CapEff|Seccomp):", "/proc/1/status"], vec![]).await?;
        commands.insert("init_status", init_status.clone());
        let exec_status = host_docker(socket, config, &["exec", &id, "/bin/busybox", "grep", "-E", "^(CapEff|Seccomp):", "/proc/self/status"], vec![]).await?;
        commands.insert("exec_status", exec_status.clone());
        let (init_capabilities, init_seccomp) = values::capability_status(docker_stdout(&init_status)?)
            .context("container init capability/seccomp observation")?;
        let (capabilities, seccomp) = values::capability_status(docker_stdout(&exec_status)?)
            .context("container exec capability/seccomp observation")?;
        ensure!(init_capabilities == capabilities && init_seccomp == seccomp, "init/exec capability or seccomp observations differ");
        let setup = host_docker(socket, config, &["exec", &id, "/bin/busybox", "sh", "-c", SETUP], vec![]).await?;
        commands.insert("node_creation_and_null", setup.clone());
        ensure!(docker_stdout(&setup)? == "character special file:1:3\ncharacter special file:a:ed\n", "node identity or null baseline mismatch");
        let opened = host_docker(socket, config, &["exec", &id, "/bin/busybox", "dd", "if=/dev/vz-policy-loop-control", "of=/dev/null", "count=0"], vec![]).await?;
        commands.insert("loop_control_open", opened.clone());
        if allow_loop_control {
            ensure!(docker_stdout(&opened)?.is_empty(), "unexpected positive-control stdout");
        } else {
            values::denied_open(&opened)?;
        }
        Ok::<Value, anyhow::Error>(json!({
            "container_id": id, "device_cgroup_rule": if allow_loop_control { Some(RULE) } else { None },
            "cap_eff": capabilities, "seccomp": seccomp, "cap_mknod_observed": true,
            "init_cap_eff": init_capabilities, "exec_cap_eff": capabilities,
            "init_seccomp": init_seccomp, "exec_seccomp": seccomp,
            "null_create_and_write": true, "loop_control_node_created": true, "loop_control_major": 10, "loop_control_minor": 237,
            "loop_control_open_allowed": allow_loop_control, "errno_symbolic": if allow_loop_control { None } else { Some("EPERM") },
            "numeric_errno_measured": false,
        }))
    }.await;
    let cleanup = host_docker(socket, config, &["rm", "-f", &id], vec![]).await;
    let cleanup_check = match cleanup {
        Ok(value) => {
            commands.insert("cleanup", value.clone());
            docker_stdout(&value)
                .and_then(namespace_values::container_id)
                .and_then(|removed| {
                    ensure!(removed == id, "cleanup ID mismatch");
                    Ok(())
                })
        }
        Err(error) => Err(error.context("exact-ID cleanup transport")),
    };
    match (proof, cleanup_check) {
        (Ok(mut proof), Ok(())) => {
            proof["commands"] = serde_json::to_value(commands)?;
            proof["cleanup_confirmed"] = json!(true);
            Ok(proof)
        }
        (proof, cleanup) => Err(anyhow!(
            "device policy case failed: allow_loop_control={allow_loop_control}; proof={proof:?}; cleanup={cleanup:?}; commands={commands:?}"
        )),
    }
}

/// Every command uses the unmodified host Docker and the exact Machine endpoint.
/// The control changes only one device-cgroup rule, retaining default caps and
/// seccomp. Node creation is permitted; the denied operation under test is open.
pub async fn prove(
    activation: &Arc<MachineRuntimeActivation>,
    socket: &Path,
    config: &Path,
) -> Result<Value> {
    let baseline = case(socket, config, false).await?;
    let control = case(socket, config, true)
        .await
        .map_err(|error| anyhow!("positive control failed: {error:#}; baseline={baseline}"))?;
    ensure!(
        baseline["cap_eff"] == control["cap_eff"] && baseline["seccomp"] == control["seccomp"],
        "paired capability/seccomp observations differ: baseline={baseline}; control={control}"
    );
    Ok(json!({
        "schema_version": 2, "scope": "DEV_host_docker_differential_device_open_policy",
        "owner": activation.owner(), "runtime_identity": activation.runtime_identity(),
        "endpoint": format!("unix://{}", socket.display()), "runtime": "youki",
        "sole_policy_difference": {"device_cgroup_rule": RULE},
        "privileged_or_runtime_overrides": false, "default_capabilities_and_seccomp": true,
        "matrix": {"default_policy": baseline, "explicit_device_rule_control": control},
        "default_device_open_policy_enforced": true,
        "numeric_errno_measured": false, "cleanup_confirmed": true,
    }))
}
