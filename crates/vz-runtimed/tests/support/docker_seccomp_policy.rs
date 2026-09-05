//! Differential syscall behavior through the unmodified host Docker client.
//! BusyBox readlink reports a denied syscall as exit 1 without stderr. The
//! paired default-policy command proves this exact proc link normally resolves.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;

use super::{docker_namespace_values::container_id, docker_stdout, host_docker};

pub const PROFILE: &str = r#"{"defaultAction":"SCMP_ACT_ALLOW","syscalls":[{"names":["readlinkat"],"action":"SCMP_ACT_ERRNO","errnoRet":13}]}"#;
const IMAGE: &str = "vz-endpoint-fixture:local";
const INSPECT: &str =
    r#"{"state":{{json .State}},"host_config":{{json .HostConfig}},"id":{{json .Id}}}"#;

pub(super) async fn case(
    socket: &Path,
    config: &Path,
    tenant: bool,
    custom: bool,
) -> Result<Value> {
    let directory = tempfile::Builder::new()
        .prefix("seccomp-policy-")
        .tempdir_in(config)?;
    let cid_path = directory.path().join("container.id");
    let profile_path = directory.path().join("profile.json");
    std::fs::write(&profile_path, PROFILE)?;
    let security = format!("seccomp={}", profile_path.display());
    let mut args = vec![
        "run",
        "--network",
        "none",
        "--env",
        "LC_ALL=C",
        "--cidfile",
        cid_path.to_str().context("UTF-8 cidfile")?,
    ];
    if tenant {
        args.push("--detach");
    }
    if custom {
        args.extend(["--security-opt", &security]);
    }
    args.extend([IMAGE, "/bin/busybox"]);
    args.extend(if tenant {
        ["sleep", "300"]
    } else {
        ["readlink", "/proc/self/exe"]
    });
    let run = host_docker(socket, config, &args, vec![]).await;
    let mut commands = BTreeMap::new();
    if let Ok(value) = &run {
        commands.insert("run", value.clone());
    }
    let cid_file = std::fs::read_to_string(&cid_path);
    let id = cid_file.as_ref().ok().and_then(|value| container_id(value).ok())
        .or_else(|| {
            tenant.then_some(())?;
            run.as_ref().ok().and_then(|value| docker_stdout(value).ok())
                .and_then(|stdout| container_id(stdout).ok())
        })
        .map(str::to_owned).ok_or_else(|| anyhow!(
            "no validated seccomp cleanup ID: run={run:?}; cidfile={cid_file:?}; cleanup cannot be claimed"))?;
    let proof = async {
        let run = run?;
        let observed = if tenant {
            ensure!(
                container_id(docker_stdout(&run)?)? == id,
                "run/cidfile mismatch"
            );
            let executed = host_docker(
                socket,
                config,
                &["exec", &id, "/bin/busybox", "readlink", "/proc/self/exe"],
                vec![],
            )
            .await?;
            commands.insert("exec", executed.clone());
            executed
        } else {
            run
        };
        ensure!(
            observed["exit_code"] == if custom { 1 } else { 0 },
            "syscall result mismatch: {observed}"
        );
        ensure!(
            observed["stdout"] == if custom { "" } else { "/bin/busybox\n" }
                && observed["stderr"] == "",
            "unexpected syscall output: {observed}"
        );
        let inspected = host_docker(
            socket,
            config,
            &["inspect", "--format", INSPECT, &id],
            vec![],
        )
        .await?;
        commands.insert("inspect", inspected.clone());
        let inspection: Value = serde_json::from_str(docker_stdout(&inspected)?)?;
        ensure!(
            inspection["id"] == id && inspection["state"]["Running"] == tenant,
            "wrong container state"
        );
        ensure!(
            inspection["state"]["ExitCode"] == if custom && !tenant { 1 } else { 0 },
            "unexpected container init exit"
        );
        let state = &inspection["state"];
        ensure!(
            state["Error"] == "" && state["Status"] == if tenant { "running" } else { "exited" },
            "container failed outside the syscall fixture"
        );
        for flag in ["Dead", "Paused", "Restarting", "OOMKilled"] {
            ensure!(state[flag] == false, "unexpected container {flag}");
        }
        ensure!(
            state["Pid"].as_u64().is_some_and(|pid| if tenant {
                pid > 1 && pid <= i32::MAX as u64
            } else {
                pid == 0
            }),
            "container PID contradicts fixture lifecycle"
        );
        let host = &inspection["host_config"];
        ensure!(
            host["Runtime"] == "youki"
                && host["Privileged"] == false
                && host["NetworkMode"] == "none",
            "unexpected runtime/privilege/network"
        );
        ensure!(
            host["ContainerIDFile"] == cid_path.to_str().context("UTF-8 cidfile")?,
            "inspect cidfile mismatch"
        );
        for field in ["CapAdd", "CapDrop", "Devices", "DeviceCgroupRules"] {
            ensure!(
                host.get(field).is_some_and(
                    |value| value.is_null() || value.as_array().is_some_and(Vec::is_empty)
                ),
                "unexpected {field} override"
            );
        }
        if custom {
            let options = host["SecurityOpt"]
                .as_array()
                .context("custom security options")?;
            ensure!(options.len() == 1, "extra custom security options");
            let profile: Value = serde_json::from_str(
                options[0]
                    .as_str()
                    .context("security option text")?
                    .strip_prefix("seccomp=")
                    .context("not seccomp policy")?,
            )?;
            ensure!(
                profile == serde_json::from_str::<Value>(PROFILE)?,
                "Engine did not retain exact profile"
            );
        } else {
            ensure!(
                host.get("SecurityOpt").is_some_and(
                    |value| value.is_null() || value.as_array().is_some_and(Vec::is_empty)
                ),
                "default policy overridden"
            );
        }
        Ok::<_, anyhow::Error>(
            json!({"container_id": id, "tenant": tenant, "custom_policy": custom,
            "syscall_allowed": !custom, "numeric_errno_measured": false}),
        )
    }
    .await;
    let cleanup = host_docker(socket, config, &["rm", "-f", &id], vec![]).await;
    let cleaned = match cleanup {
        Ok(value) => {
            commands.insert("cleanup", value.clone());
            docker_stdout(&value)
                .and_then(container_id)
                .and_then(|removed| {
                    ensure!(removed == id, "seccomp cleanup ID mismatch");
                    Ok(())
                })
        }
        Err(error) => Err(error),
    };
    match (proof, cleaned) {
        (Ok(mut proof), Ok(())) => {
            proof["commands"] = serde_json::to_value(commands)?;
            proof["cleanup_confirmed"] = json!(true);
            Ok(proof)
        }
        (proof, cleanup) => Err(anyhow!(
            "seccomp case failed: tenant={tenant}, custom={custom}; proof={proof:?}; cleanup={cleanup:?}; commands={commands:?}"
        )),
    }
}

pub async fn prove(
    activation: &Arc<MachineRuntimeActivation>,
    socket: &Path,
    config: &Path,
) -> Result<Value> {
    let mut matrix = BTreeMap::new();
    for (name, tenant, custom) in [
        ("default_init", false, false),
        ("custom_init", false, true),
        ("default_exec", true, false),
        ("custom_exec", true, true),
    ] {
        let result = case(socket, config, tenant, custom)
            .await
            .map_err(|error| {
                anyhow!("seccomp matrix failed at {name}: {error:#}; previous={matrix:?}")
            })?;
        matrix.insert(name, result);
    }
    let mut ids = BTreeSet::new();
    let mut expected_host = None;
    for case in matrix.values() {
        ensure!(
            ids.insert(case["container_id"].as_str().context("case ID")?),
            "seccomp matrix reused a container"
        );
        let inspection: Value = serde_json::from_str(docker_stdout(&case["commands"]["inspect"])?)?;
        let mut host = inspection["host_config"]
            .as_object()
            .context("HostConfig")?
            .clone();
        host.remove("SecurityOpt");
        host.remove("ContainerIDFile");
        if let Some(expected) = &expected_host {
            ensure!(
                &host == expected,
                "seccomp matrix changed another HostConfig policy"
            );
        } else {
            expected_host = Some(host);
        }
    }
    Ok(
        json!({"schema_version": 1, "scope": "DEV_host_docker_default_and_custom_seccomp_init_exec",
        "owner": activation.owner(), "runtime_identity": activation.runtime_identity(),
        "endpoint": format!("unix://{}", socket.display()), "runtime": "youki",
        "profile": PROFILE, "profile_sha256": format!("{:x}", Sha256::digest(PROFILE.as_bytes())),
        "matrix": matrix, "cleanup_confirmed": true}),
    )
}
