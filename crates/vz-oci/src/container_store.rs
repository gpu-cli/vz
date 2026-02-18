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
                rootfs_path: Some(PathBuf::from("/tmp/example")),
                host_pid: Some(12345),
            })
            .unwrap();

        store.remove("container-1").unwrap();

        let remaining = store.load_all().unwrap();
        assert!(remaining.is_empty());
        assert!(store.remove("container-1").is_err());
    }
}
