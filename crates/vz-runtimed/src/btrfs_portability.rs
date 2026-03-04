#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::{collections::HashSet, ffi::OsString};

use vz_runtime_contract::MachineErrorCode;
use vz_stack::StackError;

fn machine_error(code: MachineErrorCode, message: impl Into<String>) -> StackError {
    StackError::Machine {
        code,
        message: message.into(),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn export_subvolume_send_stream(
    _subvolume_path: &Path,
    _stream_path: &Path,
) -> Result<(), StackError> {
    Err(machine_error(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "btrfs send is only supported on Linux; current platform is {}",
            std::env::consts::OS
        ),
    ))
}

#[cfg(target_os = "linux")]
pub fn export_subvolume_send_stream(
    subvolume_path: &Path,
    stream_path: &Path,
) -> Result<(), StackError> {
    if !subvolume_path.is_dir() {
        return Err(machine_error(
            MachineErrorCode::ValidationError,
            format!(
                "btrfs send source subvolume does not exist or is not a directory: {}",
                subvolume_path.display()
            ),
        ));
    }
    if let Some(parent) = stream_path.parent() {
        fs::create_dir_all(parent).map_err(StackError::from)?;
    }
    let stream_file = File::create(stream_path).map_err(StackError::from)?;
    let timestamp_nanos = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    let snapshot_name = format!(".vz-send-{}-{timestamp_nanos}", std::process::id());
    let snapshot_path = subvolume_path
        .parent()
        .ok_or_else(|| {
            machine_error(
                MachineErrorCode::ValidationError,
                format!(
                    "btrfs send source subvolume has no parent directory: {}",
                    subvolume_path.display()
                ),
            )
        })?
        .join(snapshot_name);

    let snapshot_output = Command::new("btrfs")
        .args([
            "subvolume",
            "snapshot",
            "-r",
            &subvolume_path.to_string_lossy(),
            &snapshot_path.to_string_lossy(),
        ])
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;
    if !snapshot_output.status.success() {
        return Err(machine_error(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs readonly snapshot failed for {}: {}",
                subvolume_path.display(),
                String::from_utf8_lossy(&snapshot_output.stderr).trim()
            ),
        ));
    }

    let send_output = Command::new("btrfs")
        .args(["send", &snapshot_path.to_string_lossy()])
        .stdout(Stdio::from(stream_file))
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;

    let cleanup_output = Command::new("btrfs")
        .args(["subvolume", "delete", &snapshot_path.to_string_lossy()])
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;
    if !cleanup_output.status.success() {
        return Err(machine_error(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs delete failed for snapshot {}: {}",
                snapshot_path.display(),
                String::from_utf8_lossy(&cleanup_output.stderr).trim()
            ),
        ));
    }

    if send_output.status.success() {
        return Ok(());
    }
    Err(machine_error(
        MachineErrorCode::BackendUnavailable,
        format!(
            "btrfs send failed for {}: {}",
            snapshot_path.display(),
            String::from_utf8_lossy(&send_output.stderr).trim()
        ),
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn import_subvolume_receive_stream(
    _stream_path: &Path,
    _receive_parent: &Path,
) -> Result<PathBuf, StackError> {
    Err(machine_error(
        MachineErrorCode::UnsupportedOperation,
        format!(
            "btrfs receive is only supported on Linux; current platform is {}",
            std::env::consts::OS
        ),
    ))
}

#[cfg(target_os = "linux")]
fn subvolume_directory_entries(path: &Path) -> Result<HashSet<OsString>, StackError> {
    let mut entries = HashSet::new();
    for item in fs::read_dir(path).map_err(StackError::from)? {
        let item = item.map_err(StackError::from)?;
        if item.file_type().map_err(StackError::from)?.is_dir() {
            entries.insert(item.file_name());
        }
    }
    Ok(entries)
}

#[cfg(target_os = "linux")]
pub fn import_subvolume_receive_stream(
    stream_path: &Path,
    receive_parent: &Path,
) -> Result<PathBuf, StackError> {
    if !stream_path.is_file() {
        return Err(machine_error(
            MachineErrorCode::ValidationError,
            format!(
                "btrfs receive stream file not found: {}",
                stream_path.display()
            ),
        ));
    }
    fs::create_dir_all(receive_parent).map_err(StackError::from)?;
    let before = subvolume_directory_entries(receive_parent)?;
    let stream_file = File::open(stream_path).map_err(StackError::from)?;
    let output = Command::new("btrfs")
        .args(["receive", &receive_parent.to_string_lossy()])
        .stdin(Stdio::from(stream_file))
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;
    if !output.status.success() {
        return Err(machine_error(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs receive failed into {}: {}",
                receive_parent.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    let after = subvolume_directory_entries(receive_parent)?;
    let mut created: Vec<OsString> = after.difference(&before).cloned().collect();
    created.sort();
    if created.len() != 1 {
        return Err(machine_error(
            MachineErrorCode::StateConflict,
            format!(
                "expected exactly one received subvolume under {}, found {}",
                receive_parent.display(),
                created.len()
            ),
        ));
    }
    let received_subvolume_path = receive_parent.join(&created[0]);
    let writable_subvolume_path =
        receive_parent.join(format!("{}-rw", created[0].to_string_lossy()));
    let snapshot_output = Command::new("btrfs")
        .args([
            "subvolume",
            "snapshot",
            &received_subvolume_path.to_string_lossy(),
            &writable_subvolume_path.to_string_lossy(),
        ])
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;
    if !snapshot_output.status.success() {
        return Err(machine_error(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs snapshot failed from {} to {}: {}",
                received_subvolume_path.display(),
                writable_subvolume_path.display(),
                String::from_utf8_lossy(&snapshot_output.stderr).trim()
            ),
        ));
    }

    let delete_received_output = Command::new("btrfs")
        .args([
            "subvolume",
            "delete",
            &received_subvolume_path.to_string_lossy(),
        ])
        .stderr(Stdio::piped())
        .output()
        .map_err(StackError::from)?;
    if !delete_received_output.status.success() {
        return Err(machine_error(
            MachineErrorCode::BackendUnavailable,
            format!(
                "btrfs delete failed for received subvolume {}: {}",
                received_subvolume_path.display(),
                String::from_utf8_lossy(&delete_received_output.stderr).trim()
            ),
        ));
    }

    Ok(writable_subvolume_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn portability_ops_reject_non_linux() {
        let err = export_subvolume_send_stream(Path::new("/tmp/src"), Path::new("/tmp/out.stream"))
            .expect_err("non-linux export should fail");
        assert!(
            err.to_string().contains("only supported on Linux"),
            "error should mention Linux-only support"
        );
        let err = import_subvolume_receive_stream(Path::new("/tmp/in.stream"), Path::new("/tmp"))
            .expect_err("non-linux import should fail");
        assert!(
            err.to_string().contains("only supported on Linux"),
            "error should mention Linux-only support"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn import_requires_stream_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let err = import_subvolume_receive_stream(
            &temp_dir.path().join("missing.stream"),
            temp_dir.path(),
        )
        .expect_err("missing stream file should fail");
        assert!(err.to_string().contains("stream file not found"));
    }
}
