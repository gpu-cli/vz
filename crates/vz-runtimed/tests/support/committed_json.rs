//! Atomic, checksum-bound JSON handoff shared by physical integration tests.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

pub fn write_json(path: &Path, value: &serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize JSON evidence");
    write_new_file_atomically(path, &bytes);
}

pub fn write_committed_json(path: &Path, value: &serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize committed JSON");
    write_new_file_atomically(path, &bytes);
    let checksum = format!("{:x}", Sha256::digest(&bytes));
    write_new_file_atomically(&json_commit_path(path), checksum.as_bytes());
}

fn write_new_file_atomically(path: &Path, bytes: &[u8]) {
    try_write_new_file_atomically(path, bytes, |_| {}).expect("atomically publish new JSON file");
}

pub fn try_write_new_file_atomically(
    path: &Path,
    bytes: &[u8],
    before_publish: impl FnOnce(&Path),
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = path.parent().expect("atomic file has parent");
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("atomic file has UTF-8 name");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    before_publish(&temporary);

    // A hard-link is an atomic, no-replace publication. The payload is synced
    // before it becomes visible; syncing the directory makes the name durable.
    if let Err(error) = std::fs::hard_link(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    File::open(parent)?.sync_all()?;
    std::fs::remove_file(&temporary)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn json_commit_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("committed JSON has UTF-8 name");
    path.with_file_name(format!("{file_name}.ready"))
}

pub async fn read_committed_json<T: DeserializeOwned>(path: &Path, timeout: Duration) -> T {
    let commit_path = json_commit_path(path);
    wait_for_file(&commit_path, timeout).await;
    let checksum = std::fs::read_to_string(&commit_path).expect("read committed JSON checksum");
    let bytes = std::fs::read(path).expect("read committed JSON payload");
    assert_eq!(
        checksum,
        format!("{:x}", Sha256::digest(&bytes)),
        "committed JSON checksum mismatch for {}",
        path.display()
    );
    serde_json::from_slice(&bytes).expect("decode committed JSON payload")
}

async fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if path.is_file() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("file was not created in time: {}", path.display());
}
