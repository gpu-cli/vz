//! Real runtime adapter backed by `vz_oci_macos::Runtime`.
//!
//! Bridges the validation harness to real Linux VMs via
//! Virtualization.framework. Supports single-container (S1-S5)
//! and multi-service compose (S6) scenarios.

use std::path::Path;
use std::time::Duration;

use tracing::{debug, info};
use vz_oci_macos::{ExecConfig, ExecutionMode, NetworkServiceConfig, RunConfig, Runtime, RuntimeConfig};

use crate::cohort::ImageRef;
use crate::runner::{ExecOutput, RuntimeAdapter};
use crate::scenario::Scenario;

/// Runtime adapter that executes scenarios on real Linux VMs.
pub struct OciRuntimeAdapter {
    runtime: Runtime,
    handle: tokio::runtime::Handle,
}

impl OciRuntimeAdapter {
    /// Create a new adapter with a data directory for image/rootfs storage.
    pub fn new(data_dir: &Path) -> Self {
        let config = RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            require_exact_agent_version: false,
            agent_ready_timeout: Duration::from_secs(15),
            exec_timeout: Duration::from_secs(60),
            ..RuntimeConfig::default()
        };
        Self {
            runtime: Runtime::new(config),
            handle: tokio::runtime::Handle::current(),
        }
    }

    /// Create an adapter with a custom `RuntimeConfig`.
    pub fn with_config(config: RuntimeConfig) -> Self {
        Self {
            runtime: Runtime::new(config),
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl RuntimeAdapter for OciRuntimeAdapter {
    fn execute(&self, image: &ImageRef, scenario: &Scenario) -> Result<ExecOutput, String> {
        self.handle.block_on(async {
            // 1. Pull image.
            info!("Pulling image: {}", image.reference);
            self.runtime
                .pull(&image.reference)
                .await
                .map_err(|e| format!("pull failed for {}: {e}", image.reference))?;

            let mut lifecycle = Vec::new();

            // 2. Build RunConfig from scenario.
            let env: Vec<(String, String)> = scenario
                .environment
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let cmd = scenario
                .command
                .clone()
                .or_else(|| scenario.entrypoint.clone())
                .unwrap_or_default();

            let run_config = RunConfig {
                cmd,
                working_dir: scenario.working_dir.clone(),
                env,
                user: scenario.user.clone(),
                execution_mode: ExecutionMode::OciRuntime,
                ..RunConfig::default()
            };

            // 3. Create container.
            lifecycle.push("create".to_string());
            let container_id = self
                .runtime
                .create_container(&image.reference, run_config)
                .await
                .map_err(|e| format!("create_container failed: {e}"))?;

            lifecycle.push("start".to_string());

            // 4. If scenario has a command to exec (separate from init process),
            //    exec it inside the running container.
            let (exec_exit, exec_stdout, exec_stderr) = if let Some(ref cmd) = scenario.command {
                lifecycle.push("exec".to_string());
                let exec_cfg = ExecConfig {
                    cmd: cmd.clone(),
                    working_dir: scenario.working_dir.clone(),
                    env: scenario
                        .environment
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    user: scenario.user.clone(),
                    timeout: Some(Duration::from_secs(30)),
                };
                let output = self
                    .runtime
                    .exec_container(&container_id, exec_cfg)
                    .await
                    .map_err(|e| format!("exec_container failed: {e}"))?;
                (output.exit_code, output.stdout, output.stderr)
            } else {
                // No explicit command — wait briefly for the init process.
                tokio::time::sleep(Duration::from_secs(2)).await;
                (0, String::new(), String::new())
            };

            // 5. Stop + remove container.
            let _ = self.runtime.stop_container(&container_id, false).await;
            lifecycle.push("stop".to_string());
            let _ = self.runtime.remove_container(&container_id).await;
            lifecycle.push("delete".to_string());

            // 6. Build validation ExecOutput.
            Ok(ExecOutput {
                exit_code: exec_exit,
                stdout: exec_stdout,
                stderr: exec_stderr,
                lifecycle_events: lifecycle,
            })
        })
    }

    fn execute_compose(&self, scenario: &Scenario) -> Result<ExecOutput, String> {
        let services = scenario
            .compose_services
            .as_ref()
            .ok_or_else(|| format!("scenario {} missing compose_services", scenario.id))?;

        self.handle.block_on(async {
            let mut lifecycle = Vec::new();
            let stack_id = format!("val-{}", scenario.id);

            // 1. Pull all unique images.
            let mut seen_images = std::collections::HashSet::new();
            for svc in services {
                if seen_images.insert(svc.image.clone()) {
                    info!("Pulling image for service {}: {}", svc.name, svc.image);
                    self.runtime
                        .pull(&svc.image)
                        .await
                        .map_err(|e| format!("pull failed for {}: {e}", svc.image))?;
                }
            }

            // 2. Boot shared VM.
            info!("Booting shared VM for stack: {stack_id}");
            self.runtime
                .boot_shared_vm(&stack_id, vec![], Default::default())
                .await
                .map_err(|e| format!("boot_shared_vm failed: {e}"))?;

            lifecycle.push("compose-up".to_string());

            // 3. Set up per-service networking.
            let net_configs: Vec<NetworkServiceConfig> = services
                .iter()
                .enumerate()
                .map(|(i, svc)| NetworkServiceConfig {
                    name: svc.name.clone(),
                    addr: format!("172.20.0.{}/24", i + 2),
                })
                .collect();

            self.runtime
                .network_setup(&stack_id, net_configs.clone())
                .await
                .map_err(|e| format!("network_setup failed: {e}"))?;

            // 4. Create containers for each service.
            let mut container_ids: Vec<(String, String)> = Vec::new(); // (service_name, container_id)

            // Build /etc/hosts entries for cross-service DNS.
            let hosts: Vec<(String, String)> = net_configs
                .iter()
                .map(|nc| {
                    let ip = nc.addr.split('/').next().unwrap_or(&nc.addr).to_string();
                    (nc.name.clone(), ip)
                })
                .collect();

            for svc in services {
                let cmd = svc
                    .command
                    .clone()
                    .unwrap_or_else(|| vec!["sleep".to_string(), "300".to_string()]);

                let netns_path = format!("/var/run/netns/{}", svc.name);

                let run_config = RunConfig {
                    cmd,
                    execution_mode: ExecutionMode::OciRuntime,
                    extra_hosts: hosts.clone(),
                    network_namespace_path: Some(netns_path),
                    ..RunConfig::default()
                };

                debug!("Creating container for service: {}", svc.name);
                let cid = self
                    .runtime
                    .create_container_in_stack(&stack_id, &svc.image, run_config)
                    .await
                    .map_err(|e| format!("create_container_in_stack failed for {}: {e}", svc.name))?;

                container_ids.push((svc.name.clone(), cid));
            }

            lifecycle.push("service-ready".to_string());

            // 5. Run connectivity checks if defined.
            let mut check_failures = Vec::new();
            if let Some(checks) = &scenario.connectivity_checks {
                for check in checks {
                    let Some((_name, cid)) = container_ids
                        .iter()
                        .find(|(name, _)| name == &check.from_service)
                    else {
                        check_failures.push(format!(
                            "service {} not found for connectivity check",
                            check.from_service
                        ));
                        continue;
                    };

                    debug!(
                        "Running connectivity check from {}: {:?}",
                        check.from_service, check.command
                    );
                    match self
                        .runtime
                        .exec_container(
                            cid,
                            ExecConfig {
                                cmd: check.command.clone(),
                                timeout: Some(Duration::from_secs(10)),
                                ..ExecConfig::default()
                            },
                        )
                        .await
                    {
                        Ok(output) => {
                            if output.exit_code != check.expected_exit_code {
                                check_failures.push(format!(
                                    "connectivity check from {} failed: expected exit {}, got {} (stderr: {})",
                                    check.from_service,
                                    check.expected_exit_code,
                                    output.exit_code,
                                    output.stderr.trim(),
                                ));
                            }
                        }
                        Err(e) => {
                            check_failures.push(format!(
                                "connectivity check from {} errored: {e}",
                                check.from_service,
                            ));
                        }
                    }
                }
            }

            lifecycle.push("compose-healthy".to_string());

            // 6. Tear down.
            let service_names: Vec<String> = services.iter().map(|s| s.name.clone()).collect();
            let _ = self
                .runtime
                .network_teardown(&stack_id, service_names)
                .await;
            let _ = self.runtime.shutdown_shared_vm(&stack_id).await;

            let exit_code = if check_failures.is_empty() { 0 } else { 1 };
            let stderr = check_failures.join("\n");

            Ok(ExecOutput {
                exit_code,
                stdout: String::new(),
                stderr,
                lifecycle_events: lifecycle,
            })
        })
    }
}
