use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

/// Mock container runtime for testing.
///
/// Records all operations and can be configured to fail specific calls.
/// Supports shared VM tracking for multi-service stack testing.
/// Uses `Mutex`/`AtomicUsize` instead of `RefCell`/`Cell` so it is
/// `Send + Sync` and can be used with parallel container creation.
pub struct MockContainerRuntime {
    /// Container IDs to return on create calls (fallback when config has no container_id).
    pub container_ids: Vec<String>,
    /// Whether pull should fail.
    pub fail_pull: bool,
    /// Whether create should fail.
    pub fail_create: bool,
    /// Whether a failed create was admitted and owns a durable generation.
    pub claim_failed_create_ownership: bool,
    /// Whether a claimed failed-create proof should emulate legacy unscoped data.
    pub omit_failed_create_ownership_scope: bool,
    /// Whether successful creates should emulate a legacy ownership-less runtime.
    pub omit_successful_create_ownership: bool,
    /// Generation reported for admitted failed creates.
    pub failed_create_generation: u64,
    /// Runtime-ID substitutions used to exercise malformed ownership responses.
    pub failed_create_ownership_id_overrides: Mutex<HashMap<String, String>>,
    /// Whether generation-qualified cleanup should fail.
    pub fail_generation_cleanup: bool,
    /// Whether successful generation cleanup reports the target already absent.
    pub generation_cleanup_already_absent: bool,
    /// Whether stop should fail.
    pub fail_stop: bool,
    /// Whether remove should fail.
    pub fail_remove: bool,
    /// Whether remove should report the container as already absent.
    pub remove_not_found: bool,
    /// Exit code to return from exec calls.
    pub exec_exit_code: i32,
    /// Whether exec should fail with an error (not just non-zero exit).
    pub fail_exec: bool,
    /// Optional delay before returning from exec (for timeout testing).
    pub exec_delay: Option<Duration>,
    /// Tracks calls: (operation, arg).
    pub calls: Mutex<Vec<(String, String)>>,
    /// Counter for create calls (fallback ID generation).
    create_counter: AtomicUsize,
    /// Tracks which stacks have an active sandbox.
    sandboxes: Mutex<HashSet<String>>,
    /// Captured RunConfigs from create/create_in_sandbox calls, keyed by container_id.
    pub captured_configs: Mutex<Vec<(String, vz_runtime_contract::RunConfig)>>,
    /// Captured NetworkServiceConfigs from setup_sandbox_network calls.
    pub captured_network_services:
        Mutex<Vec<(String, Vec<vz_runtime_contract::NetworkServiceConfig>)>>,
    /// Container IDs to return from `list_containers`.
    pub listed_containers: Mutex<Vec<String>>,
    /// Pre-configured log lines returned by `stream_logs`.
    pub mock_log_lines: Mutex<Vec<LogLine>>,
    /// Whether exact scoped activation should fail after reservation admission.
    pub fail_scoped_activation: bool,
    /// Exact runtime IDs whose scoped activation should fail.
    pub fail_scoped_activation_ids: Mutex<HashSet<String>>,
    /// Force reservation inspection to report a foreign owner.
    pub force_foreign_scoped_inspection: bool,
    /// Durable scoped generations, keyed by requested container ID.
    scoped_generations: Mutex<HashMap<String, ScopedGeneration>>,
    /// Monotonic runtime generation allocator for scoped reservations.
    next_scoped_generation: AtomicU64,
}

#[derive(Clone)]
struct ScopedGeneration {
    ownership: vz_runtime_contract::ContainerGenerationOwnership,
    published: bool,
}

impl MockContainerRuntime {
    pub fn new() -> Self {
        Self {
            container_ids: vec!["ctr-001".to_string()],
            fail_pull: false,
            fail_create: false,
            claim_failed_create_ownership: false,
            omit_failed_create_ownership_scope: false,
            omit_successful_create_ownership: false,
            failed_create_generation: 41,
            failed_create_ownership_id_overrides: Mutex::new(HashMap::new()),
            fail_generation_cleanup: false,
            generation_cleanup_already_absent: false,
            fail_stop: false,
            fail_remove: false,
            remove_not_found: false,
            exec_exit_code: 0,
            fail_exec: false,
            exec_delay: None,
            calls: Mutex::new(Vec::new()),
            create_counter: AtomicUsize::new(0),
            sandboxes: Mutex::new(HashSet::new()),
            captured_configs: Mutex::new(Vec::new()),
            captured_network_services: Mutex::new(Vec::new()),
            listed_containers: Mutex::new(Vec::new()),
            mock_log_lines: Mutex::new(Vec::new()),
            fail_scoped_activation: false,
            fail_scoped_activation_ids: Mutex::new(HashSet::new()),
            force_foreign_scoped_inspection: false,
            scoped_generations: Mutex::new(HashMap::new()),
            next_scoped_generation: AtomicU64::new(1),
        }
    }

    pub fn with_ids(ids: Vec<&str>) -> Self {
        Self {
            container_ids: ids.into_iter().map(String::from).collect(),
            ..Self::new()
        }
    }

    pub fn call_log(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    pub fn override_failed_create_ownership_id(&self, requested: &str, returned: &str) {
        self.failed_create_ownership_id_overrides
            .lock()
            .unwrap()
            .insert(requested.to_string(), returned.to_string());
    }

    pub fn scoped_generation_count(&self) -> usize {
        self.scoped_generations.lock().unwrap().len()
    }

    pub fn scoped_ownership(
        &self,
        container_id: &str,
    ) -> Option<vz_runtime_contract::ContainerGenerationOwnership> {
        self.scoped_generations
            .lock()
            .unwrap()
            .get(container_id)
            .map(|record| record.ownership.clone())
    }

    pub fn insert_scoped_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        published: bool,
    ) {
        self.scoped_generations.lock().unwrap().insert(
            ownership.container_id.clone(),
            ScopedGeneration {
                ownership,
                published,
            },
        );
    }

    /// Generate a deterministic container ID from the RunConfig.
    ///
    /// Uses the service network-namespace basename so existing executor tests
    /// retain short, readable fixture IDs even though production requests now
    /// carry stack-namespaced runtime IDs. Falls back to the requested runtime
    /// ID, then cycles through `container_ids`.
    fn next_id(&self, config: &vz_runtime_contract::RunConfig) -> String {
        config
            .network_namespace_path
            .as_deref()
            .and_then(|path| path.rsplit('/').next())
            .filter(|name| !name.is_empty())
            .map(|name| format!("ctr-{name}"))
            .or_else(|| {
                config
                    .container_id
                    .as_ref()
                    .map(|name| format!("ctr-{name}"))
            })
            .unwrap_or_else(|| {
                let idx = self.create_counter.fetch_add(1, Ordering::SeqCst);
                self.container_ids[idx % self.container_ids.len()].clone()
            })
    }
}

impl ContainerRuntime for MockContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("pull".to_string(), image.to_string()));
        if self.fail_pull {
            return Err(StackError::InvalidSpec("mock pull failure".to_string()));
        }
        Ok(format!("sha256:{image}"))
    }

    fn create(
        &self,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("create".to_string(), image.to_string()));
        if self.fail_create {
            return Err(StackError::InvalidSpec("mock create failure".to_string()));
        }
        let id = self.next_id(&config);
        self.captured_configs
            .lock()
            .unwrap()
            .push((id.clone(), config));
        Ok(id)
    }

    fn stop(
        &self,
        container_id: &str,
        _signal: Option<&str>,
        _grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("stop".to_string(), container_id.to_string()));
        if self.fail_stop {
            return Err(StackError::InvalidSpec("mock stop failure".to_string()));
        }
        Ok(())
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("remove".to_string(), container_id.to_string()));
        if self.remove_not_found {
            return Err(StackError::Network(format!(
                "container '{container_id}' not found"
            )));
        }
        if self.fail_remove {
            return Err(StackError::InvalidSpec("mock remove failure".to_string()));
        }
        Ok(())
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        self.calls.lock().unwrap().push((
            "exec".to_string(),
            format!("{container_id}:{}", command.join(" ")),
        ));
        if let Some(delay) = self.exec_delay {
            std::thread::sleep(delay);
        }
        if self.fail_exec {
            return Err(StackError::InvalidSpec("mock exec failure".to_string()));
        }
        Ok(self.exec_exit_code)
    }

    fn stream_logs(
        &self,
        container_id: &str,
        service_name: &str,
        follow: bool,
    ) -> Result<LogStream, StackError> {
        self.calls.lock().unwrap().push((
            "stream_logs".to_string(),
            format!("{container_id}:{service_name}:follow={follow}"),
        ));
        let (tx, rx) = std::sync::mpsc::channel();
        // Send any pre-configured mock lines, then drop the sender.
        let mock_lines = self.mock_log_lines.lock().unwrap().clone();
        for line in mock_lines {
            let _ = tx.send(line);
        }
        // Sender is dropped here, closing the stream.
        Ok(rx)
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<vz_runtime_contract::PortMapping>,
        _resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        self.calls.lock().unwrap().push((
            "create_sandbox".to_string(),
            format!(
                "{}:{}",
                sandbox_id,
                ports
                    .iter()
                    .map(|p| format!("{}:{}", p.host, p.container))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ));
        self.sandboxes
            .lock()
            .unwrap()
            .insert(sandbox_id.to_string());
        Ok(())
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<String, StackError> {
        self.calls.lock().unwrap().push((
            "create_in_sandbox".to_string(),
            format!("{sandbox_id}:{image}"),
        ));
        if self.fail_create {
            return Err(StackError::InvalidSpec("mock create failure".to_string()));
        }
        let id = self.next_id(&config);
        self.captured_configs
            .lock()
            .unwrap()
            .push((id.clone(), config));
        Ok(id)
    }

    fn create_in_sandbox_owned(
        &self,
        sandbox_id: &str,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        let requested_id = config.container_id.clone();
        self.create_in_sandbox(sandbox_id, image, config)
            .map(|container_id| {
                let ownership = (!self.omit_successful_create_ownership).then(|| {
                    vz_runtime_contract::ContainerGenerationOwnership {
                        container_id: container_id.clone(),
                        generation: 1,
                        stack_id: sandbox_id.to_string(),
                        scope: Some(Box::new(
                            vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(
                                sandbox_id,
                            )
                            .expect("test sandbox ID must form a valid legacy scope"),
                        )),
                    }
                });
                vz_runtime_contract::ContainerCreateReceipt {
                    container_id,
                    ownership,
                }
            })
            .map_err(|error| vz_runtime_contract::OwnedCreateError {
                error,
                cleanup: self.claim_failed_create_ownership.then(|| {
                    let requested_id =
                        requested_id.expect("owned create test must request a container ID");
                    let container_id = self
                        .failed_create_ownership_id_overrides
                        .lock()
                        .unwrap()
                        .get(&requested_id)
                        .cloned()
                        .unwrap_or(requested_id);
                    vz_runtime_contract::ContainerGenerationOwnership {
                        container_id,
                        generation: self.failed_create_generation,
                        stack_id: sandbox_id.to_string(),
                        scope: (!self.omit_failed_create_ownership_scope).then(|| {
                            Box::new(
                                vz_runtime_contract::ContainerGenerationScope::synthetic_legacy_stack(
                                    sandbox_id,
                                )
                                .expect("test sandbox ID must form a valid legacy scope"),
                            )
                        }),
                    }
                }),
            })
    }

    fn reserve_container_generation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationOwnership, StackError> {
        let mut generations = self.scoped_generations.lock().unwrap();
        if let Some(existing) = generations.get(container_id) {
            if existing.ownership.scope.as_deref() == Some(scope) {
                return Ok(existing.ownership.clone());
            }
            return Err(scope_state_conflict(
                "mock container ID has foreign ownership",
            ));
        }
        let ownership = vz_runtime_contract::ContainerGenerationOwnership {
            container_id: container_id.to_string(),
            generation: self.next_scoped_generation.fetch_add(1, Ordering::SeqCst),
            stack_id: scope.stack_id.clone(),
            scope: Some(Box::new(scope.clone())),
        };
        generations.insert(
            container_id.to_string(),
            ScopedGeneration {
                ownership: ownership.clone(),
                published: false,
            },
        );
        self.calls
            .lock()
            .unwrap()
            .push(("reserve_scoped".to_string(), container_id.to_string()));
        Ok(ownership)
    }

    fn inspect_container_reservation(
        &self,
        scope: &vz_runtime_contract::ContainerGenerationScope,
        container_id: &str,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        if self.force_foreign_scoped_inspection {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Foreign);
        }
        let generations = self.scoped_generations.lock().unwrap();
        let Some(record) = generations.get(container_id) else {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Absent);
        };
        if record.ownership.scope.as_deref() != Some(scope) {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Foreign);
        }
        Ok(if record.published {
            vz_runtime_contract::ContainerGenerationInspection::Published(record.ownership.clone())
        } else {
            vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(
                record.ownership.clone(),
            )
        })
    }

    fn inspect_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationInspection, StackError> {
        let generations = self.scoped_generations.lock().unwrap();
        let Some(record) = generations.get(&ownership.container_id) else {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Absent);
        };
        if record.ownership.scope != ownership.scope {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Foreign);
        }
        if record.ownership.generation != ownership.generation {
            return Ok(vz_runtime_contract::ContainerGenerationInspection::Replacement);
        }
        Ok(if record.published {
            vz_runtime_contract::ContainerGenerationInspection::Published(record.ownership.clone())
        } else {
            vz_runtime_contract::ContainerGenerationInspection::ReservedUnpublished(
                record.ownership.clone(),
            )
        })
    }

    fn activate_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        image: &str,
        config: vz_runtime_contract::RunConfig,
    ) -> Result<
        vz_runtime_contract::ContainerCreateReceipt,
        vz_runtime_contract::OwnedCreateError<StackError>,
    > {
        self.calls.lock().unwrap().push((
            "activate_scoped".to_string(),
            ownership.container_id.clone(),
        ));
        let mut generations = self.scoped_generations.lock().unwrap();
        let exact_unpublished = generations
            .get(&ownership.container_id)
            .is_some_and(|record| record.ownership == ownership && !record.published);
        if !exact_unpublished {
            return Err(vz_runtime_contract::OwnedCreateError::unowned(
                scope_state_conflict("mock activation lacks exact unpublished reservation"),
            ));
        }
        if self.fail_scoped_activation
            || self
                .fail_scoped_activation_ids
                .lock()
                .unwrap()
                .contains(&ownership.container_id)
        {
            return Err(vz_runtime_contract::OwnedCreateError {
                error: StackError::InvalidSpec("mock scoped activation failure".to_string()),
                cleanup: Some(ownership),
            });
        }
        generations
            .get_mut(&ownership.container_id)
            .expect("checked scoped generation")
            .published = true;
        self.captured_configs
            .lock()
            .unwrap()
            .push((ownership.container_id.clone(), config));
        self.calls
            .lock()
            .unwrap()
            .push(("activated_image".to_string(), image.to_string()));
        Ok(vz_runtime_contract::ContainerCreateReceipt {
            container_id: ownership.container_id.clone(),
            ownership: Some(ownership),
        })
    }

    fn release_container_reservation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::ContainerGenerationReleaseOutcome, StackError> {
        let mut generations = self.scoped_generations.lock().unwrap();
        match generations.get(&ownership.container_id) {
            None => Ok(vz_runtime_contract::ContainerGenerationReleaseOutcome::AlreadyAbsent),
            Some(record) if record.ownership == ownership && !record.published => {
                generations.remove(&ownership.container_id);
                self.calls
                    .lock()
                    .unwrap()
                    .push(("release_scoped".to_string(), ownership.container_id));
                Ok(vz_runtime_contract::ContainerGenerationReleaseOutcome::Released)
            }
            _ => Err(scope_state_conflict(
                "mock release lacks exact unpublished ownership",
            )),
        }
    }

    fn cleanup_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.calls.lock().unwrap().push((
            "cleanup_container_generation".to_string(),
            format!(
                "{}:{}:{}",
                ownership.stack_id, ownership.container_id, ownership.generation
            ),
        ));
        if self.fail_generation_cleanup {
            return Err(StackError::InvalidSpec(
                "mock generation cleanup failure".to_string(),
            ));
        }
        if self.generation_cleanup_already_absent {
            Ok(vz_runtime_contract::GenerationCleanupOutcome::AlreadyAbsent)
        } else {
            Ok(vz_runtime_contract::GenerationCleanupOutcome::Removed)
        }
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: vz_runtime_contract::ContainerGenerationOwnership,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<vz_runtime_contract::GenerationCleanupOutcome, StackError> {
        self.calls.lock().unwrap().push((
            "stop_and_remove_container_generation".to_string(),
            format!(
                "{}:{}:{}:signal={}:grace_ms={}",
                ownership.stack_id,
                ownership.container_id,
                ownership.generation,
                signal.unwrap_or("<default>"),
                grace_period.map_or(0, |duration| duration.as_millis())
            ),
        ));
        if self.fail_generation_cleanup {
            return Err(StackError::InvalidSpec(
                "mock generation cleanup failure".to_string(),
            ));
        }
        if self.generation_cleanup_already_absent {
            Ok(vz_runtime_contract::GenerationCleanupOutcome::AlreadyAbsent)
        } else {
            let mut generations = self.scoped_generations.lock().unwrap();
            if generations
                .get(&ownership.container_id)
                .is_some_and(|record| record.ownership == ownership)
            {
                generations.remove(&ownership.container_id);
            }
            Ok(vz_runtime_contract::GenerationCleanupOutcome::Removed)
        }
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<vz_runtime_contract::NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        self.calls.lock().unwrap().push((
            "setup_sandbox_network".to_string(),
            format!(
                "{}:{}",
                sandbox_id,
                services
                    .iter()
                    .map(|s| format!("{}={}@{}", s.name, s.addr, s.network_name))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ));
        self.captured_network_services
            .lock()
            .unwrap()
            .push((sandbox_id.to_string(), services));
        Ok(())
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        self.calls.lock().unwrap().push((
            "teardown_sandbox_network".to_string(),
            format!("{}:{}", sandbox_id, service_names.join(",")),
        ));
        Ok(())
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("shutdown_sandbox".to_string(), sandbox_id.to_string()));
        self.sandboxes.lock().unwrap().remove(sandbox_id);
        Ok(())
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        self.sandboxes.lock().unwrap().contains(sandbox_id)
    }

    fn list_containers(&self, sandbox_id: &str) -> Result<Vec<String>, StackError> {
        self.calls
            .lock()
            .unwrap()
            .push(("list_containers".to_string(), sandbox_id.to_string()));
        Ok(self.listed_containers.lock().unwrap().clone())
    }
}
