use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// Whether stop should fail.
    pub fail_stop: bool,
    /// Whether remove should fail.
    pub fail_remove: bool,
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
}

impl MockContainerRuntime {
    pub fn new() -> Self {
        Self {
            container_ids: vec!["ctr-001".to_string()],
            fail_pull: false,
            fail_create: false,
            fail_stop: false,
            fail_remove: false,
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

    /// Generate a deterministic container ID from the RunConfig.
    ///
    /// Uses `config.container_id` (set to service name by the executor)
    /// so that IDs are deterministic regardless of parallel execution order.
    /// Falls back to cycling through `container_ids` if not set.
    fn next_id(&self, config: &vz_runtime_contract::RunConfig) -> String {
        config
            .container_id
            .as_ref()
            .map(|name| format!("ctr-{name}"))
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
