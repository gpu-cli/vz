//! Physical proof of default Docker time-namespace isolation and exec joining.
//! Offset configuration and user-namespace combinations are deliberately unproved.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;

use super::{docker_stdout, guest, host_docker};

use super::docker_namespace_values::{container_id, time_namespace};

const IMAGE: &str = "vz-endpoint-fixture:local";
const INSPECT: &str =
    r#"{"state":{{json .State}},"runtime":{{json .HostConfig.Runtime}},"id":{{json .Id}}}"#;

/// Uses only the unmodified Mac Docker client against this exact Machine.
/// A private cidfile preserves the precise cleanup target when run fails after
/// creation. All later failures still attempt exact-ID removal and retain the
/// command evidence in the returned error; no broad name/label cleanup is used.
pub async fn prove(
    activation: &Arc<MachineRuntimeActivation>,
    socket: &Path,
    config: &Path,
) -> Result<Value> {
    let cid_directory = tempfile::Builder::new()
        .prefix("time-ns-")
        .tempdir_in(config)
        .context("private Docker time-namespace cidfile directory")?;
    let cid_path = cid_directory.path().join("container.id");
    let cid_argument = cid_path.to_str().context("UTF-8 cidfile path")?;
    let run = host_docker(
        socket,
        config,
        &[
            "run",
            "--detach",
            "--network",
            "none",
            "--cidfile",
            cid_argument,
            IMAGE,
            "/bin/busybox",
            "sleep",
            "60",
        ],
        vec![],
    )
    .await;
    let mut commands = BTreeMap::new();
    let mut observations = Vec::<Value>::new();
    if let Ok(value) = &run {
        commands.insert("run", value.clone());
    }

    // Docker owns the generated ID; never turn an unvalidated stdout/error
    // fragment into a command argument or a guest /proc path.
    let cid_file = std::fs::read_to_string(&cid_path);
    let id = match cid_file
        .as_ref()
        .ok()
        .and_then(|value| container_id(value).ok())
    {
        Some(id) => id.to_string(),
        None => {
            // A successful run's complete ID is also an exact cleanup target.
            let stdout_id = run
                .as_ref()
                .ok()
                .and_then(|value| docker_stdout(value).ok())
                .and_then(|stdout| container_id(stdout).ok());
            match stdout_id {
                Some(id) => id.to_string(),
                None => {
                    return Err(anyhow!(
                        "Docker time-namespace run did not supply a validated cleanup ID; \
                     cleanup cannot be claimed: run={run:?}, cidfile={cid_file:?}, commands={commands:?}"
                    ));
                }
            }
        }
    };

    let proof = async {
        let run = run.context("host Docker time-namespace run transport")?;
        ensure!(
            container_id(docker_stdout(&run)?)? == id,
            "run stdout/cidfile ID mismatch"
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
            inspection["id"] == id,
            "inspect returned a different container"
        );
        ensure!(
            inspection["runtime"] == "youki",
            "container did not use youki"
        );
        ensure!(
            inspection["state"]["Running"] == true,
            "container is not running"
        );
        let pid = inspection["state"]["Pid"]
            .as_u64()
            .context("container init PID")?;
        ensure!(
            pid > 1 && pid <= i32::MAX as u64,
            "invalid container init PID"
        );

        let executed = host_docker(
            socket,
            config,
            &[
                "exec",
                &id,
                "/bin/busybox",
                "readlink",
                "/proc/self/ns/time",
            ],
            vec![],
        )
        .await?;
        commands.insert("exec", executed.clone());
        if executed["exit_code"] != 0 {
            // The shim's error parser loses youki's differently named JSON
            // fields. Retain bounded exact-container logs before cleanup removes
            // the bundle; no guest client substitutes for the failed host exec.
            let script = format!(
                "set +e; /bin/busybox ls -la /run/vz-docker/containerd/io.containerd.runtime.v2.task/moby/{id}; /bin/busybox tail -c 16384 /run/vz-docker/containerd/io.containerd.runtime.v2.task/moby/{id}/log.json; exit 0"
            );
            let diagnostics = guest(activation, &script).await;
            return Err(anyhow!("host Docker exec failed: {executed}; exact-container runtime log={diagnostics:?}"));
        }
        let exec_namespace = time_namespace(docker_stdout(&executed)?)?.to_string();

        let init_script = format!("/bin/busybox readlink /proc/{pid}/ns/time");
        let init_observation = guest(activation, &init_script).await?;
        observations.push(json!({"script": init_script, "stdout": init_observation}));
        let init_namespace = time_namespace(&init_observation)?.to_string();
        let guest_script = "/bin/busybox readlink /proc/1/ns/time";
        let guest_observation = guest(activation, guest_script).await?;
        observations.push(json!({"script": guest_script, "stdout": guest_observation}));
        let guest_namespace = time_namespace(&guest_observation)?.to_string();

        ensure!(
            exec_namespace == init_namespace,
            "Docker exec did not join its container time namespace"
        );
        ensure!(
            init_namespace != guest_namespace,
            "default Docker container shares guest-init time namespace"
        );
        let root_filesystem =
            super::docker_exec_root::prove(activation, socket, config, &id, pid).await?;
        Ok::<Value, anyhow::Error>(json!({
            "schema_version": 1,
            "scope": "host_docker_default_time_namespace_and_exec_only",
            "time_offsets_tested": false,
            "namespace_overrides_used": false,
            "owner": activation.owner(),
            "runtime_identity": activation.runtime_identity(),
            "endpoint": format!("unix://{}", socket.display()),
            "container_id": id,
            "container_init_pid": pid,
            "runtime": "youki",
            "container_init_time_namespace": init_namespace,
            "exec_time_namespace": exec_namespace,
            "guest_init_time_namespace": guest_namespace,
            "container_time_namespace_isolated": true,
            "exec_joined_container_time_namespace": true,
            "root_filesystem": root_filesystem,
        }))
    }
    .await;

    // This is deliberately outside the fallible proof block. An inspect,
    // exec, guest observation, or equality failure cannot skip cleanup.
    let cleanup = host_docker(socket, config, &["rm", "-f", &id], vec![]).await;
    let cleanup_check = match &cleanup {
        Ok(value) => {
            commands.insert("cleanup", value.clone());
            docker_stdout(value)
                .and_then(container_id)
                .and_then(|removed| {
                    ensure!(removed == id, "cleanup returned a different container ID");
                    Ok(())
                })
        }
        Err(error) => Err(anyhow!("cleanup transport failed: {error:#}")),
    };
    match (proof, cleanup_check) {
        (Ok(mut proof), Ok(())) => {
            proof["commands"] = serde_json::to_value(commands)?;
            proof["guest_observations"] = json!(observations);
            proof["cleanup_confirmed"] = json!(true);
            Ok(proof)
        }
        (proof, cleanup) => Err(anyhow!(
            "Docker default-time/exec proof failed: proof={proof:?}; cleanup={cleanup:?}; \
             commands={commands:?}; guest_observations={observations:?}"
        )),
    }
}
