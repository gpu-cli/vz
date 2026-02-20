//! Filesystem-backed container metadata registry.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Runtime status for a tracked container.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContainerStatus {
    /// Container metadata created, but execution hasn't started yet.
    Created,
    /// Container is currently running.
    Running,
    /// Container exited with an exit code.
    Stopped {
        /// Exit code from the container command.
        exit_code: i32,
    },
}

/// Serializable container metadata record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerInfo {
    /// Container identifier.
    pub id: String,
    /// Original image reference used for creation.
    pub image: String,
    /// Resolved image digest identifier.
    pub image_id: String,
    /// Container lifecycle status.
    pub status: ContainerStatus,
    /// Unix epoch seconds when metadata was created.
    pub created_unix_secs: u64,
    /// Unix epoch seconds when the container was started, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_unix_secs: Option<u64>,
    /// Unix epoch seconds when the container stopped, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_unix_secs: Option<u64>,
    /// Assembled rootfs path for this container, when known.
    pub rootfs_path: Option<PathBuf>,
    /// Host process ID currently managing this container, if running.
    pub host_pid: Option<u32>,
}

/// Persistent metadata index for containers.
#[derive(Debug, Clone)]
pub struct ContainerStore {
    base_dir: PathBuf,
}

impl ContainerStore {
    /// Create a container store rooted at `base_dir`.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Load all container metadata records.
    pub fn load_all(&self) -> io::Result<Vec<ContainerInfo>> {
        let path = self.containers_json_path();

        if !path.exists() {
            return Ok(Vec::new());
        }

        let data = fs::read(&path)?;
        if data.is_empty() {
            return Ok(Vec::new());
        }

        serde_json::from_slice(&data).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid containers.json: {err}"),
            )
        })
    }

    /// Insert or replace a container metadata record by ID.
    pub fn upsert(&self, container: ContainerInfo) -> io::Result<()> {
        let mut containers = self.load_all()?;

        match containers.iter().position(|item| item.id == container.id) {
            Some(index) => containers[index] = container,
            None => containers.push(container),
        }

        containers.sort_by(|a, b| a.id.cmp(&b.id));
        self.write_all(&containers)
    }

    /// Find a single container by ID.
    pub fn find(&self, id: &str) -> io::Result<Option<ContainerInfo>> {
        let containers = self.load_all()?;
        Ok(containers.into_iter().find(|c| c.id == id))
    }

    /// Reconcile stale containers whose host PID is no longer alive.
    ///
    /// Containers in `Running` or `Created` state whose `host_pid` no longer
    /// exists are transitioned to `Stopped { exit_code: -1 }` with their
    /// rootfs cleaned up. Returns the IDs of reconciled containers.
    pub fn reconcile_stale(&self) -> io::Result<Vec<String>> {
        let mut containers = self.load_all()?;
        let mut reconciled = Vec::new();

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());

        for container in &mut containers {
            let is_active = matches!(
                container.status,
                ContainerStatus::Running | ContainerStatus::Created
            );
            if !is_active {
                continue;
            }

            let pid_alive = container.host_pid.is_some_and(is_process_alive);

            if !pid_alive {
                container.status = ContainerStatus::Stopped { exit_code: -1 };
                container.stopped_unix_secs = Some(now_secs);
                container.host_pid = None;

                if let Some(rootfs) = container.rootfs_path.take() {
                    let _ = fs::remove_dir_all(rootfs);
                }

                reconciled.push(container.id.clone());
            }
        }

        if !reconciled.is_empty() {
            self.write_all(&containers)?;
        }

        Ok(reconciled)
    }

    /// Remove a container metadata record by ID.
    pub fn remove(&self, id: &str) -> io::Result<()> {
        let mut containers = self.load_all()?;
        let len = containers.len();
        containers.retain(|container| container.id != id);

        if len == containers.len() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("container '{id}' not found"),
            ));
        }

        self.write_all(&containers)
    }

    fn containers_json_path(&self) -> PathBuf {
        self.base_dir.join("containers.json")
    }

    fn write_all(&self, containers: &[ContainerInfo]) -> io::Result<()> {
        let path = self.containers_json_path();
        let bytes = serde_json::to_vec_pretty(containers)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        write_atomic(&path, &bytes)
    }
}

fn write_atomic(destination: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = unique_temp_path(destination);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }

    fs::rename(&tmp, destination)
}

/// Check if a process with the given PID is alive.
fn is_process_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("containers.json");
    let temp_name = format!("{file_name}.tmp.{pid}.{timestamp}");
    let mut out = path.to_path_buf();
    out.set_file_name(temp_name);

    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::env;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        base.push(format!(
            "vz-oci-container-store-test-{name}-{}-{}",
            std::process::id(),
            nanos.as_nanos(),
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn load_all_returns_empty_when_file_is_missing() {
        let root = unique_temp_dir("missing");
        let store = ContainerStore::new(root);

        let containers = store.load_all().unwrap();
        assert!(containers.is_empty());
    }

    #[test]
    fn upsert_replaces_existing_records() {
        let root = unique_temp_dir("upsert");
        let store = ContainerStore::new(root);

        store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:base".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 1700,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:base".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 1700,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let containers = store.load_all().unwrap();

        assert_eq!(containers.len(), 1);
        assert_eq!(containers[0].id, "container-1");
        assert!(matches!(
            containers[0].status,
            ContainerStatus::Stopped { exit_code: 0 }
        ));
    }

    #[test]
    fn remove_deletes_record() {
        let root = unique_temp_dir("remove");
        let store = ContainerStore::new(root);

        store
            .upsert(ContainerInfo {
                id: "container-1".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:base".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 1700,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: Some(PathBuf::from("/tmp/example")),
                host_pid: Some(12345),
            })
            .unwrap();

        store.remove("container-1").unwrap();

        let remaining = store.load_all().unwrap();
        assert!(remaining.is_empty());
        assert!(store.remove("container-1").is_err());
    }

    #[test]
    fn find_returns_matching_container() {
        let root = unique_temp_dir("find");
        let store = ContainerStore::new(root);

        store
            .upsert(ContainerInfo {
                id: "ctr-a".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:a".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(std::process::id()),
            })
            .unwrap();

        let found = store.find("ctr-a").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, "ctr-a");

        let missing = store.find("ctr-none").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn reconcile_stale_transitions_dead_pid_containers() {
        let root = unique_temp_dir("reconcile");
        let store = ContainerStore::new(root);

        // Container with a PID that definitely doesn't exist.
        store
            .upsert(ContainerInfo {
                id: "stale-running".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:a".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(999_999_999),
            })
            .unwrap();

        // Container with our own PID — should remain running.
        store
            .upsert(ContainerInfo {
                id: "alive-running".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:b".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 200,
                started_unix_secs: Some(201),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(std::process::id()),
            })
            .unwrap();

        // Already stopped container — should be untouched.
        store
            .upsert(ContainerInfo {
                id: "already-stopped".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:c".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 50,
                started_unix_secs: Some(51),
                stopped_unix_secs: Some(60),
                rootfs_path: None,
                host_pid: None,
            })
            .unwrap();

        let reconciled = store.reconcile_stale().unwrap();

        assert_eq!(reconciled, vec!["stale-running".to_string()]);

        let containers = store.load_all().unwrap();
        let stale = containers.iter().find(|c| c.id == "stale-running").unwrap();
        assert!(matches!(
            stale.status,
            ContainerStatus::Stopped { exit_code: -1 }
        ));
        assert!(stale.stopped_unix_secs.is_some());
        assert!(stale.host_pid.is_none());

        let alive = containers.iter().find(|c| c.id == "alive-running").unwrap();
        assert!(matches!(alive.status, ContainerStatus::Running));
        assert_eq!(alive.host_pid, Some(std::process::id()));
    }

    #[test]
    fn reconcile_stale_cleans_up_rootfs() {
        let root = unique_temp_dir("reconcile-rootfs");
        let store = ContainerStore::new(root.clone());

        let rootfs_dir = root.join("stale-rootfs");
        fs::create_dir_all(&rootfs_dir).unwrap();

        store
            .upsert(ContainerInfo {
                id: "stale-with-rootfs".to_string(),
                image: "ubuntu:24.04".to_string(),
                image_id: "sha256:a".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 100,
                started_unix_secs: Some(101),
                stopped_unix_secs: None,
                rootfs_path: Some(rootfs_dir.clone()),
                host_pid: Some(999_999_999),
            })
            .unwrap();

        let reconciled = store.reconcile_stale().unwrap();
        assert_eq!(reconciled.len(), 1);
        assert!(!rootfs_dir.exists());
    }

    #[test]
    fn serde_round_trip_with_new_timestamp_fields() {
        let original = ContainerInfo {
            id: "ctr-serde".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:serde".to_string(),
            status: ContainerStatus::Stopped { exit_code: 42 },
            created_unix_secs: 1000,
            started_unix_secs: Some(1001),
            stopped_unix_secs: Some(1010),
            rootfs_path: None,
            host_pid: None,
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ContainerInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, original);
    }

    #[test]
    fn serde_backward_compat_missing_timestamp_fields() {
        // Simulate old JSON without started_unix_secs/stopped_unix_secs.
        let json = r#"{
            "id": "old-ctr",
            "image": "ubuntu:24.04",
            "image_id": "sha256:old",
            "status": "Created",
            "created_unix_secs": 500,
            "rootfs_path": null,
            "host_pid": null
        }"#;

        let info: ContainerInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.id, "old-ctr");
        assert!(info.started_unix_secs.is_none());
        assert!(info.stopped_unix_secs.is_none());
    }
}
