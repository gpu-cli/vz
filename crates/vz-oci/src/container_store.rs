//! Filesystem-backed container metadata registry.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Kernel-backed per-container lifecycle lease.
///
/// The lock file is persistent and never unlinked, keeping a stable inode for
/// correct advisory-lock behavior across independent runtime processes.
pub struct ContainerIdLease {
    file: File,
    lock_path: PathBuf,
    exclusive: bool,
}

impl Drop for ContainerIdLease {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

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

/// Durable ownership token for one incarnation of a caller-selected container ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerGeneration(pub u64);

/// Read-only durable generation state for lifecycle diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerGenerationDiagnostic {
    /// Caller-selected container ID.
    pub container_id: String,
    /// Monotonic durable generation.
    pub generation: ContainerGeneration,
    /// Whether the name is currently reserved.
    pub reserved: bool,
    /// Host process that created the reservation.
    pub owner_pid: u32,
    /// Best-effort current liveness of `owner_pid`.
    pub owner_alive: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct GenerationRecord {
    generation: u64,
    reserved: bool,
    #[serde(default)]
    owner_pid: u32,
}

impl ContainerStore {
    /// Create a container store rooted at `base_dir`.
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Acquire shared lifecycle admission for an exec operation.
    pub fn acquire_container_read_lease(&self, id: &str) -> io::Result<ContainerIdLease> {
        let lock_path = self.container_lease_path(id);
        let file = self.open_container_lease_file(id)?;
        fs2::FileExt::lock_shared(&file)?;
        Ok(ContainerIdLease {
            file,
            lock_path,
            exclusive: false,
        })
    }

    /// Acquire exclusive lifecycle admission for stop/remove.
    pub fn acquire_container_write_lease(&self, id: &str) -> io::Result<ContainerIdLease> {
        let lock_path = self.container_lease_path(id);
        let file = self.open_container_lease_file(id)?;
        fs2::FileExt::lock_exclusive(&file)?;
        Ok(ContainerIdLease {
            file,
            lock_path,
            exclusive: true,
        })
    }

    /// Try to acquire exclusive admission for a new generation.
    ///
    /// Unlike stop/remove, duplicate creation must fail rather than inherit a
    /// name after the current transaction completes.
    pub fn try_acquire_container_write_lease(&self, id: &str) -> io::Result<ContainerIdLease> {
        let lock_path = self.container_lease_path(id);
        let file = self.open_container_lease_file(id)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(ContainerIdLease {
                file,
                lock_path,
                exclusive: true,
            }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("container '{id}' already has an in-flight lifecycle transaction"),
            )),
            Err(error) => Err(error),
        }
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
    ///
    /// Acquires an advisory file lock to serialize concurrent access.
    pub fn upsert(&self, container: ContainerInfo) -> io::Result<()> {
        let _lock = self.lock()?;

        let mut containers = self.load_all()?;

        match containers.iter().position(|item| item.id == container.id) {
            Some(index) => containers[index] = container,
            None => containers.push(container),
        }

        containers.sort_by(|a, b| a.id.cmp(&b.id));
        self.write_all(&containers)
    }

    /// Atomically reserve an absent container ID and allocate its next generation.
    ///
    /// A stopped record still owns its name until explicit removal. This preserves
    /// the stop + remove + same-name recreate contract while rejecting duplicates.
    pub fn reserve_generation(&self, id: &str) -> io::Result<ContainerGeneration> {
        self.reserve_generation_inner(id, false)
    }

    /// Reserve a generation while holding this ID's exclusive OS lifecycle lease.
    ///
    /// The lease is authoritative evidence that no create transaction still owns
    /// an unpublished reservation, so a stale sidecar is reclaimed regardless of
    /// PID reuse after a process crash.
    pub fn reserve_generation_with_write_lease(
        &self,
        id: &str,
        lease: &ContainerIdLease,
    ) -> io::Result<ContainerGeneration> {
        if !lease.exclusive || lease.lock_path != self.container_lease_path(id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("exclusive lifecycle lease does not belong to container '{id}'"),
            ));
        }
        self.reserve_generation_inner(id, true)
    }

    fn reserve_generation_inner(
        &self,
        id: &str,
        reclaim_unpublished: bool,
    ) -> io::Result<ContainerGeneration> {
        let _lock = self.lock()?;
        if self.load_all()?.iter().any(|container| container.id == id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("container '{id}' already exists; stop and remove it before recreating"),
            ));
        }

        let mut records = self.load_generations()?;
        if !reclaim_unpublished
            && records.get(id).is_some_and(|record| {
                record.reserved && record.owner_pid != 0 && is_process_alive(record.owner_pid)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("container '{id}' already has an in-flight generation"),
            ));
        }
        let generation = records
            .get(id)
            .map_or(1, |record| record.generation.saturating_add(1));
        records.insert(
            id.to_string(),
            GenerationRecord {
                generation,
                reserved: true,
                owner_pid: std::process::id(),
            },
        );
        self.write_generations(&records)?;
        Ok(ContainerGeneration(generation))
    }

    /// Return the currently reserved generation for an existing container.
    pub fn current_generation(&self, id: &str) -> io::Result<Option<ContainerGeneration>> {
        let _lock = self.lock()?;
        let mut records = self.load_generations()?;
        if let Some(record) = records.get(id) {
            return Ok(record
                .reserved
                .then_some(ContainerGeneration(record.generation)));
        }
        if !self.load_all()?.iter().any(|container| container.id == id) {
            return Ok(None);
        }
        // Adopt metadata written by older versions into generation one.
        records.insert(
            id.to_string(),
            GenerationRecord {
                generation: 1,
                reserved: true,
                owner_pid: std::process::id(),
            },
        );
        self.write_generations(&records)?;
        Ok(Some(ContainerGeneration(1)))
    }

    /// Snapshot all durable generation reservations without mutating them.
    pub fn generation_diagnostics(&self) -> io::Result<Vec<ContainerGenerationDiagnostic>> {
        let _lock = self.lock()?;
        let mut diagnostics: Vec<_> = self
            .load_generations()?
            .into_iter()
            .map(|(container_id, record)| ContainerGenerationDiagnostic {
                container_id,
                generation: ContainerGeneration(record.generation),
                reserved: record.reserved,
                owner_pid: record.owner_pid,
                owner_alive: record.owner_pid != 0 && is_process_alive(record.owner_pid),
            })
            .collect();
        diagnostics.sort_by(|left, right| left.container_id.cmp(&right.container_id));
        Ok(diagnostics)
    }

    /// Update metadata only while `generation` still owns `container.id`.
    pub fn upsert_if_generation(
        &self,
        container: ContainerInfo,
        generation: ContainerGeneration,
    ) -> io::Result<()> {
        let _lock = self.lock()?;
        self.require_generation(&container.id, generation)?;
        let mut containers = self.load_all()?;
        match containers.iter().position(|item| item.id == container.id) {
            Some(index) => containers[index] = container,
            None => containers.push(container),
        }
        containers.sort_by(|a, b| a.id.cmp(&b.id));
        self.write_all(&containers)
    }

    /// Remove metadata and release the name only for the owning generation.
    pub fn remove_if_generation(
        &self,
        id: &str,
        generation: ContainerGeneration,
    ) -> io::Result<()> {
        let _lock = self.lock()?;
        self.require_generation(id, generation)?;
        let mut containers = self.load_all()?;
        let len = containers.len();
        containers.retain(|container| container.id != id);
        if len == containers.len() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("container '{id}' not found"),
            ));
        }
        self.write_all(&containers)?;
        let mut records = self.load_generations()?;
        if let Some(record) = records.get_mut(id) {
            record.reserved = false;
        }
        self.write_generations(&records)
    }

    /// Release an unpublished reservation owned by `generation`.
    ///
    /// This is used by create-transaction drop paths that fail before writing a
    /// ContainerInfo. It never releases a published or replacement generation.
    pub fn release_generation_if_absent(
        &self,
        id: &str,
        generation: ContainerGeneration,
    ) -> io::Result<bool> {
        let _lock = self.lock()?;
        if self.load_all()?.iter().any(|container| container.id == id) {
            return Ok(false);
        }
        let mut records = self.load_generations()?;
        let Some(record) = records.get_mut(id) else {
            return Ok(false);
        };
        if !record.reserved || record.generation != generation.0 {
            return Ok(false);
        }
        record.reserved = false;
        self.write_generations(&records)?;
        Ok(true)
    }

    fn require_generation(&self, id: &str, generation: ContainerGeneration) -> io::Result<()> {
        let current = self.load_generations()?.get(id).copied();
        if current.is_some_and(|record| record.reserved && record.generation == generation.0) {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "container '{id}' generation {} no longer owns the name",
                generation.0
            ),
        ))
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
        let _lock = self.lock()?;

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
        let _lock = self.lock()?;

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

    fn lock_path(&self) -> PathBuf {
        self.base_dir.join("containers.lock")
    }

    fn generations_path(&self) -> PathBuf {
        self.base_dir.join("container-generations.json")
    }

    fn open_container_lease_file(&self, id: &str) -> io::Result<File> {
        let locks_dir = self.base_dir.join("container-lifecycle-locks");
        fs::create_dir_all(&locks_dir)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.container_lease_path(id))
    }

    fn container_lease_path(&self, id: &str) -> PathBuf {
        let digest = format!("{:x}", Sha256::digest(id.as_bytes()));
        self.base_dir
            .join("container-lifecycle-locks")
            .join(format!("{digest}.lock"))
    }

    fn load_generations(&self) -> io::Result<HashMap<String, GenerationRecord>> {
        let path = self.generations_path();
        if !path.exists() {
            return Ok(HashMap::new());
        }
        let data = fs::read(path)?;
        if data.is_empty() {
            return Ok(HashMap::new());
        }
        serde_json::from_slice(&data)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn write_generations(&self, records: &HashMap<String, GenerationRecord>) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(records)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        write_atomic(&self.generations_path(), &bytes)
    }

    /// Acquire an exclusive advisory lock on the container store.
    ///
    /// The lock is released when the returned guard is dropped.
    fn lock(&self) -> io::Result<FileLock> {
        fs::create_dir_all(&self.base_dir)?;
        FileLock::acquire(&self.lock_path())
    }

    fn write_all(&self, containers: &[ContainerInfo]) -> io::Result<()> {
        let path = self.containers_json_path();
        let bytes = serde_json::to_vec_pretty(containers)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        write_atomic(&path, &bytes)
    }
}

/// RAII guard for an exclusive file lock.
///
/// Uses `File::create_new` (O_EXCL) to atomically create a lock file. The
/// lock is released by deleting the file when the guard is dropped.
struct FileLock {
    path: PathBuf,
}

impl FileLock {
    /// Acquire an exclusive lock, spinning with backoff until available.
    fn acquire(path: &Path) -> io::Result<Self> {
        let mut elapsed = std::time::Duration::ZERO;
        let timeout = std::time::Duration::from_secs(30);
        let poll = std::time::Duration::from_millis(10);

        loop {
            match File::create_new(path) {
                Ok(_file) => {
                    return Ok(Self {
                        path: path.to_path_buf(),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if elapsed >= timeout {
                        // Stale lock — force remove and retry once.
                        let _ = fs::remove_file(path);
                        if File::create_new(path).is_ok() {
                            return Ok(Self {
                                path: path.to_path_buf(),
                            });
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "timed out acquiring container store lock: {}",
                                path.display()
                            ),
                        ));
                    }
                    std::thread::sleep(poll);
                    elapsed += poll;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
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

    fn generation_test_container(id: &str) -> ContainerInfo {
        ContainerInfo {
            id: id.to_string(),
            image: "alpine:latest".to_string(),
            image_id: "sha256:generation".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 1,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        }
    }

    #[test]
    fn generation_reservation_rejects_duplicate_until_explicit_remove() {
        let root = unique_temp_dir("generation-duplicate");
        let store = ContainerStore::new(root);
        let first = store.reserve_generation("same-name").unwrap();
        store
            .upsert_if_generation(generation_test_container("same-name"), first)
            .unwrap();

        assert_eq!(
            store.reserve_generation("same-name").unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        store.remove_if_generation("same-name", first).unwrap();
        let second = store.reserve_generation("same-name").unwrap();
        assert!(second.0 > first.0);
    }

    #[test]
    fn stale_generation_cannot_update_or_remove_replacement() {
        let root = unique_temp_dir("generation-stale");
        let store = ContainerStore::new(root);
        let first = store.reserve_generation("same-name").unwrap();
        store
            .upsert_if_generation(generation_test_container("same-name"), first)
            .unwrap();
        store.remove_if_generation("same-name", first).unwrap();
        let second = store.reserve_generation("same-name").unwrap();
        let mut replacement = generation_test_container("same-name");
        replacement.image_id = "sha256:replacement".to_string();
        store
            .upsert_if_generation(replacement.clone(), second)
            .unwrap();

        assert_eq!(
            store
                .upsert_if_generation(generation_test_container("same-name"), first)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            store
                .remove_if_generation("same-name", first)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(store.find("same-name").unwrap(), Some(replacement));
    }

    #[test]
    fn unpublished_reservation_can_be_released_and_retried() {
        let root = unique_temp_dir("generation-unpublished");
        let store = ContainerStore::new(root);
        let first = store.reserve_generation("same-name").unwrap();
        assert!(
            store
                .release_generation_if_absent("same-name", first)
                .unwrap()
        );
        let second = store.reserve_generation("same-name").unwrap();
        assert!(second.0 > first.0);
    }

    #[test]
    fn dead_owner_reservation_is_reaped_on_retry() {
        let root = unique_temp_dir("generation-dead-owner");
        let store = ContainerStore::new(root);
        let first = store.reserve_generation("same-name").unwrap();
        let mut records = store.load_generations().unwrap();
        records.get_mut("same-name").unwrap().owner_pid = u32::MAX;
        store.write_generations(&records).unwrap();

        let second = store.reserve_generation("same-name").unwrap();
        assert!(second.0 > first.0);
    }

    #[test]
    fn exclusive_lease_reclaims_stale_reservation_even_if_pid_is_alive() {
        let root = unique_temp_dir("generation-live-pid-stale");
        let store = ContainerStore::new(root);
        let first = store.reserve_generation("same-name").unwrap();
        let lease = store
            .try_acquire_container_write_lease("same-name")
            .unwrap();

        let second = store
            .reserve_generation_with_write_lease("same-name", &lease)
            .unwrap();
        assert!(second.0 > first.0);
    }
}
