//! Actual Mac Docker exec root-boundary proof; parent owns exact-ID cleanup.

use std::{path::Path, sync::Arc};

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use vz_runtimed::machine_runtime_activation::MachineRuntimeActivation;

use super::{docker_exec_root_values, docker_stdout, guest, host_docker};

pub async fn prove(
    activation: &Arc<MachineRuntimeActivation>,
    socket: &Path,
    config: &Path,
    id: &str,
    pid: u64,
) -> Result<Value> {
    super::docker_namespace_values::container_id(id)?;
    let script = docker_exec_root_values::guest_script(pid)?;
    let before = guest(activation, &script).await?;
    let executed = host_docker(
        socket,
        config,
        &[
            "exec",
            id,
            "/bin/busybox",
            "sh",
            "-c",
            docker_exec_root_values::EXEC_SCRIPT,
        ],
        vec![],
    )
    .await?;
    let after = guest(activation, &script).await?;
    docker_stdout(&executed)
        .and_then(|stdout| docker_exec_root_values::validate(&before, stdout, &after))
        .map_err(|error| anyhow!("Docker exec root boundary failed: {error:#}; before={before:?}; command={executed:?}; after={after:?}"))?;
    Ok(json!({
        "schema_version": 1,
        "scope": "host_docker_exec_container_root_pid_and_mount_boundary",
        "container_id": id,
        "container_init_pid": pid,
        "guest_before": {"script": script, "stdout": before},
        "exec": executed,
        "guest_after": {"script": script, "stdout": after},
        "exec_root_matches_container_init": true,
        "exec_proc_matches_container_namespaces": true,
    }))
}
