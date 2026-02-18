//! VM registry -- Tracks running VMs in `~/.vz/vms.json`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tracing::debug;

/// Registry file path: `~/.vz/vms.json`.
fn registry_path() -> PathBuf {
    vz_home().join("vms.json")
}

/// Return the vz home directory (`~/.vz`), creating it if it doesn't exist.
pub fn vz_home() -> PathBuf {
    let home = dirs_home().join(".vz");
    if !home.exists() {
        let _ = std::fs::create_dir_all(&home);
    }
    home
}

/// Platform home directory.
fn dirs_home() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/Users/Shared"))
    }

    #[cfg(not(target_os = "macos"))]
    {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp"))
    }
}

/// A single VM entry in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmEntry {
    /// Path to the disk image.
    pub image: String,

    /// VM state: "running", "stopped", "saved".
    pub state: String,

    /// PID of the process managing this VM.
    pub pid: u32,

    /// Vsock port for guest agent communication.
    #[serde(default)]
    pub vsock_port: Option<u32>,

    /// Number of CPU cores.
    #[serde(default)]
    pub cpus: Option<u32>,

    /// Memory in GB.
    #[serde(default)]
    pub memory_gb: Option<u64>,

    /// VirtioFS mounts.
    #[serde(default)]
    pub mounts: Vec<Mount>,
}

/// A VirtioFS mount entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mount {
    /// Mount tag (visible inside the VM).
    pub tag: String,

    /// Source path on the host.
    pub source: String,
}

/// In-memory representation of `~/.vz/vms.json`.
#[derive(Debug)]
pub struct Registry {
    entries: BTreeMap<String, VmEntry>,
}

impl Registry {
    /// Load the registry from disk, or create an empty one if it doesn't exist.
    pub fn load() -> anyhow::Result<Self> {
        let path = registry_path();
        if !path.exists() {
            debug!(path = %path.display(), "registry file not found, using empty registry");
            return Ok(Self {
                entries: BTreeMap::new(),
            });
        }

        let data = std::fs::read_to_string(&path)?;
        let entries: BTreeMap<String, VmEntry> = serde_json::from_str(&data)?;
        debug!(
            path = %path.display(),
            count = entries.len(),
            "loaded registry"
        );
        Ok(Self { entries })
    }

    /// Save the registry to disk.
    pub fn save(&self) -> anyhow::Result<()> {
        let path = registry_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_string_pretty(&self.entries)?;
        std::fs::write(&path, data)?;
        debug!(path = %path.display(), "saved registry");
        Ok(())
    }

    /// Get all entries.
    pub fn entries(&self) -> &BTreeMap<String, VmEntry> {
        &self.entries
    }

    /// Insert or update a VM entry.
    #[allow(dead_code)]
    pub fn insert(&mut self, name: String, entry: VmEntry) {
        self.entries.insert(name, entry);
    }

    /// Remove a VM entry by name.
    pub fn remove(&mut self, name: &str) -> Option<VmEntry> {
        self.entries.remove(name)
    }

    /// Look up a VM entry by name.
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<&VmEntry> {
        self.entries.get(name)
    }
}

/// Check whether a process with the given PID is alive.
pub fn is_pid_alive(pid: u32) -> bool {
    // kill(pid, 0) checks if the process exists without sending a signal.
    // Returns 0 if process exists (or we have permission to signal it).
    // Returns -1 with ESRCH if the process does not exist.
    #[allow(unsafe_code)]
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_empty_roundtrip() {
        let reg = Registry {
            entries: BTreeMap::new(),
        };
        assert!(reg.entries().is_empty());
    }

    #[test]
    fn registry_insert_and_get() {
        let mut reg = Registry {
            entries: BTreeMap::new(),
        };
        let entry = VmEntry {
            image: "/path/to/image.img".to_string(),
            state: "running".to_string(),
            pid: 12345,
            vsock_port: Some(7424),
            cpus: Some(4),
            memory_gb: Some(8),
            mounts: vec![Mount {
                tag: "workspace".to_string(),
                source: "/Users/dev/workspace".to_string(),
            }],
        };
        reg.insert("test-vm".to_string(), entry);
        assert!(reg.get("test-vm").is_some());
        assert_eq!(reg.get("test-vm").unwrap().pid, 12345);
    }

    #[test]
    fn registry_remove() {
        let mut reg = Registry {
            entries: BTreeMap::new(),
        };
        let entry = VmEntry {
            image: "/path/to/image.img".to_string(),
            state: "stopped".to_string(),
            pid: 0,
            vsock_port: None,
            cpus: None,
            memory_gb: None,
            mounts: vec![],
        };
        reg.insert("vm1".to_string(), entry);
        assert!(reg.get("vm1").is_some());
        reg.remove("vm1");
        assert!(reg.get("vm1").is_none());
    }

    #[test]
    fn vm_entry_serde_roundtrip() {
        let entry = VmEntry {
            image: "/images/base.img".to_string(),
            state: "running".to_string(),
            pid: 42,
            vsock_port: Some(7424),
            cpus: Some(4),
            memory_gb: Some(8),
            mounts: vec![Mount {
                tag: "workspace".to_string(),
                source: "/Users/dev/project".to_string(),
            }],
        };
        let json = serde_json::to_string(&entry).unwrap();
        let decoded: VmEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.pid, 42);
        assert_eq!(decoded.mounts.len(), 1);
        assert_eq!(decoded.mounts[0].tag, "workspace");
    }

    #[test]
    fn vm_entry_deserialize_minimal() {
        let json = r#"{"image":"/img","state":"stopped","pid":0}"#;
        let entry: VmEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.image, "/img");
        assert!(entry.vsock_port.is_none());
        assert!(entry.mounts.is_empty());
    }

    #[test]
    fn is_pid_alive_self() {
        // Our own process should be alive
        let pid = std::process::id();
        assert!(is_pid_alive(pid));
    }

    #[test]
    fn is_pid_alive_nonexistent() {
        // PID 0 is the kernel, but very high PIDs are likely unused
        // Use a PID that almost certainly doesn't exist
        assert!(!is_pid_alive(4_000_000));
    }
}
