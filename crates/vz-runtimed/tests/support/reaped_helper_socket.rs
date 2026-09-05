//! Test-only authority for an exactly owned helper's SIGKILL socket residue.
//!
//! Production bind never removes existing paths. This fixture pins a socket
//! while its owned child is live, positively reaps that same child, then removes
//! only the unchanged socket relative to its retained directory descriptor.
//! The directory is owned by this UID and not writable by other users. As with
//! the production socket guard, trusted same-UID code must not race the final
//! identity check and unlink: POSIX has no atomic compare-inode-and-unlink call.

use rustix::fs::{AtFlags, FileType, Mode, OFlags, Stat, fstat, open, statat, unlinkat};
use serde::Serialize;
use std::ffi::OsString;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

pub struct CapturedHelperSocket {
    child_id: u32,
    path: PathBuf,
    name: OsString,
    parent: OwnedFd,
    parent_identity: Stat,
    socket_identity: Stat,
}

#[derive(Debug, Serialize)]
pub struct SocketCleanupReceipt {
    child_id: u32,
    socket_path: PathBuf,
    socket_device: i64,
    socket_inode: u64,
    child_sigkill_reaped: bool,
    captured_socket_removed: bool,
}

fn rejected(message: &str) -> io::Error {
    io::Error::other(message)
}

fn trusted_parent(path: &Path) -> io::Result<(OwnedFd, Stat)> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let identity = fstat(&descriptor)?;
    if identity.st_uid != rustix::process::geteuid().as_raw() || identity.st_mode & 0o022 != 0 {
        return Err(rejected("helper socket parent is not privately controlled"));
    }
    Ok((descriptor, identity))
}

fn same_identity(left: &Stat, right: &Stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_mode == right.st_mode
        && left.st_uid == right.st_uid
        && left.st_gid == right.st_gid
        && left.st_nlink == right.st_nlink
}

impl CapturedHelperSocket {
    pub fn capture(child: &mut Child, path: &Path) -> io::Result<Self> {
        if !path.is_absolute() || child.try_wait()?.is_some() {
            return Err(rejected(
                "capture requires an absolute path and live owned child",
            ));
        }
        let parent_path = path.parent().ok_or_else(|| rejected("missing parent"))?;
        let name = path
            .file_name()
            .ok_or_else(|| rejected("missing socket name"))?;
        let (parent, parent_identity) = trusted_parent(parent_path)?;
        let socket_identity = statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if !FileType::from_raw_mode(socket_identity.st_mode).is_socket()
            || socket_identity.st_uid != rustix::process::geteuid().as_raw()
            || socket_identity.st_nlink != 1
            || socket_identity.st_mode & 0o777 != 0o600
            || child.try_wait()?.is_some()
        {
            return Err(rejected(
                "capture requires the live helper's private socket",
            ));
        }
        Ok(Self {
            child_id: child.id(),
            path: path.into(),
            name: name.into(),
            parent,
            parent_identity,
            socket_identity,
        })
    }

    pub fn kill_reap_and_remove(self, child: &mut Child) -> io::Result<SocketCleanupReceipt> {
        if child.id() != self.child_id || child.try_wait()?.is_some() {
            return Err(rejected(
                "cleanup requires the original still-owned live child",
            ));
        }
        child.kill()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait()? {
                if status.signal() != Some(9) {
                    return Err(rejected("helper was not positively reaped after SIGKILL"));
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "helper reap timed out; socket retained",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let parent_path = self
            .path
            .parent()
            .ok_or_else(|| rejected("missing parent"))?;
        let (_current_parent, current_identity) = trusted_parent(parent_path)?;
        if !same_identity(&self.parent_identity, &current_identity)
            || !same_identity(&self.parent_identity, &fstat(&self.parent)?)
        {
            return Err(rejected("helper socket parent changed; retained all paths"));
        }
        let current = statat(&self.parent, &self.name, AtFlags::SYMLINK_NOFOLLOW)?;
        if !same_identity(&self.socket_identity, &current)
            || self.socket_identity.st_ctime != current.st_ctime
            || self.socket_identity.st_ctime_nsec != current.st_ctime_nsec
        {
            return Err(rejected("helper socket changed; replacement retained"));
        }
        unlinkat(&self.parent, &self.name, AtFlags::empty())?;
        Ok(SocketCleanupReceipt {
            child_id: self.child_id,
            socket_path: self.path,
            socket_device: i64::from(self.socket_identity.st_dev),
            socket_inode: self.socket_identity.st_ino,
            child_sigkill_reaped: true,
            captured_socket_removed: true,
        })
    }
}
