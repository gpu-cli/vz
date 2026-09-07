//! Persistent startup exclusion; a released lock pathname is never unlinked.
//!
//! The daemon UID is trusted. Descriptor-relative checks reject replacement
//! across acquisition; they do not claim atomic protection against a malicious
//! same-UID actor changing names between the final check and a write.

use std::path::{Path, PathBuf};

use crate::RuntimedError;

#[cfg(unix)]
mod platform {
    use std::ffi::OsString;
    use std::fs::{File, Metadata};
    use std::io;
    use std::os::unix::fs::MetadataExt;
    use std::path::Component;

    use rustix::fs::{Mode, OFlags, open, openat};

    use super::*;

    const DIRECTORY: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::NONBLOCK)
        .union(OFlags::CLOEXEC);
    const READ: OFlags = OFlags::RDONLY
        .union(OFlags::NOFOLLOW)
        .union(OFlags::NONBLOCK)
        .union(OFlags::CLOEXEC);
    const WRITE: OFlags = OFlags::RDWR
        .union(OFlags::CREATE)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::NONBLOCK)
        .union(OFlags::CLOEXEC);

    /// Owns the advisory lock and its pinned parent for the daemon lifetime.
    /// Closing the file releases exclusion; its pathname remains persistent.
    #[derive(Debug)]
    pub(crate) struct StartupLock {
        path: PathBuf,
        original_parent: PathBuf,
        canonical_parent: PathBuf,
        name: OsString,
        parent: File,
        parent_identity: (u64, u64),
        file: File,
        file_identity: (u64, u64),
    }

    fn conflict(message: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, message)
    }

    fn identity(metadata: &Metadata) -> (u64, u64) {
        (metadata.dev(), metadata.ino())
    }

    fn require_parent(metadata: &Metadata) -> io::Result<()> {
        if !metadata.is_dir()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o022 != 0
        {
            return Err(conflict(
                "startup lock parent must be owned and not group/world writable",
            ));
        }
        Ok(())
    }

    fn require_file(metadata: &Metadata) -> io::Result<()> {
        if !metadata.is_file()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.nlink() != 1
            || metadata.mode() & 0o7777 != 0o600
        {
            return Err(conflict(
                "startup lock must be an owned single-link regular mode-0600 file",
            ));
        }
        Ok(())
    }

    impl StartupLock {
        pub(crate) fn acquire(path: PathBuf) -> Result<Self, RuntimedError> {
            let lock = Self::prepare(path.clone())
                .map_err(|source| RuntimedError::AcquireStartupLock { path, source })?;
            lock.finish()
        }

        // Separate preparation permits deterministic replacement tests without
        // introducing a public acquisition hook or changing process globals.
        fn prepare(path: PathBuf) -> io::Result<Self> {
            if path
                .components()
                .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(conflict(
                    "startup lock path must not contain parent traversal",
                ));
            }
            let name = path
                .file_name()
                .ok_or_else(|| conflict("startup lock filename missing"))?
                .to_owned();
            let original_parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .to_owned();
            let canonical_parent = original_parent.canonicalize()?;
            let parent = File::from(open(&canonical_parent, DIRECTORY, Mode::empty())?);
            let metadata = parent.metadata()?;
            require_parent(&metadata)?;
            let parent_identity = identity(&metadata);
            let file = File::from(openat(&parent, &name, WRITE, Mode::RUSR | Mode::WUSR)?);
            let metadata = file.metadata()?;
            require_file(&metadata)?;
            let lock = Self {
                path,
                original_parent,
                canonical_parent,
                name,
                parent,
                parent_identity,
                file,
                file_identity: identity(&metadata),
            };
            lock.validate()?;
            Ok(lock)
        }

        fn finish(self) -> Result<Self, RuntimedError> {
            self.validate().map_err(|source| self.error(source))?;
            if let Err(source) = fs2::FileExt::try_lock_exclusive(&self.file) {
                return Err(if source.kind() == io::ErrorKind::WouldBlock {
                    RuntimedError::StartupLockAlreadyHeld {
                        path: self.path.clone(),
                    }
                } else {
                    self.error(source)
                });
            }
            self.validate().map_err(|source| self.error(source))?;
            // The kernel lock and pinned inode are the fence. Never rewrite
            // an existing file's contents during admission; process diagnostics
            // belong to the independently admitted control-owner record.
            self.file.sync_all().map_err(|source| self.error(source))?;
            self.parent
                .sync_all()
                .map_err(|source| self.error(source))?;
            self.validate().map_err(|source| self.error(source))?;
            Ok(self)
        }

        fn error(&self, source: io::Error) -> RuntimedError {
            RuntimedError::AcquireStartupLock {
                path: self.path.clone(),
                source,
            }
        }

        fn validate(&self) -> io::Result<()> {
            if self.original_parent.canonicalize()? != self.canonical_parent {
                return Err(conflict("startup lock parent resolution changed"));
            }
            let parent = self.parent.metadata()?;
            require_parent(&parent)?;
            let named_parent = File::from(open(&self.canonical_parent, DIRECTORY, Mode::empty())?);
            let named_parent = named_parent.metadata()?;
            require_parent(&named_parent)?;
            if identity(&parent) != self.parent_identity
                || identity(&named_parent) != self.parent_identity
            {
                return Err(conflict("startup lock parent identity changed"));
            }
            let file = self.file.metadata()?;
            require_file(&file)?;
            let named_file = File::from(openat(&self.parent, &self.name, READ, Mode::empty())?);
            let named_file = named_file.metadata()?;
            require_file(&named_file)?;
            if identity(&file) != self.file_identity || identity(&named_file) != self.file_identity
            {
                return Err(conflict("startup lock file identity changed"));
            }
            Ok(())
        }

        pub(crate) fn path(&self) -> &Path {
            &self.path
        }

        /// Recheck the retained parent and the exact persistent lock inode.
        #[cfg(any(test, target_os = "macos"))]
        pub(crate) fn validate_current(&self) -> Result<(), RuntimedError> {
            self.validate().map_err(|source| self.error(source))
        }

        /// Device/inode binding for a record retained under this lock's fence.
        #[cfg(any(test, target_os = "macos"))]
        pub(crate) fn identity(&self) -> Result<(u64, u64), RuntimedError> {
            self.validate_current()?;
            Ok(self.file_identity)
        }
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::unwrap_used, clippy::expect_used)]
        use std::fs;
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};

        use super::*;

        fn fixture() -> (tempfile::TempDir, PathBuf) {
            let root = tempfile::tempdir().expect("owned temporary parent");
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = root.path().join("daemon.lock");
            (root, path)
        }

        fn existing(path: &Path) {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path)
                .unwrap();
            file.write_all(b"retained diagnostic\n").unwrap();
        }

        #[test]
        fn persistent_inode_prevents_old_opener_split_lock_after_release() {
            let (_root, path) = fixture();
            let first = StartupLock::acquire(path.clone()).unwrap();
            let original = identity(&fs::metadata(&path).unwrap());
            assert_eq!(first.identity().unwrap(), original);
            let old_opener = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            assert!(fs2::FileExt::try_lock_exclusive(&old_opener).is_err());
            drop(first);
            assert_eq!(identity(&fs::metadata(&path).unwrap()), original);
            fs2::FileExt::try_lock_exclusive(&old_opener).unwrap();
            assert!(matches!(
                StartupLock::acquire(path.clone()),
                Err(RuntimedError::StartupLockAlreadyHeld { .. })
            ));
            assert_eq!(identity(&fs::metadata(&path).unwrap()), original);
            drop(old_opener);
            let next = StartupLock::acquire(path.clone()).unwrap();
            assert_eq!(next.path(), path);
            drop(next);
            assert_eq!(identity(&fs::metadata(&path).unwrap()), original);
        }

        #[test]
        fn busy_acquisition_does_not_rewrite_diagnostic() {
            let (_root, path) = fixture();
            let lock = StartupLock::acquire(path.clone()).unwrap();
            let bytes = fs::read(&path).unwrap();
            let before = fs::metadata(&path).unwrap();
            assert!(matches!(
                StartupLock::acquire(path.clone()),
                Err(RuntimedError::StartupLockAlreadyHeld { .. })
            ));
            let after = fs::metadata(&path).unwrap();
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert_eq!(
                (before.mtime(), before.mtime_nsec()),
                (after.mtime(), after.mtime_nsec())
            );
            drop(lock);
        }

        #[test]
        fn descriptors_are_nonblocking_and_close_on_exec() {
            let (_root, path) = fixture();
            let lock = StartupLock::acquire(path).unwrap();
            for file in [&lock.file, &lock.parent] {
                assert!(
                    rustix::fs::fcntl_getfl(file)
                        .unwrap()
                        .contains(OFlags::NONBLOCK)
                );
                assert!(
                    rustix::io::fcntl_getfd(file)
                        .unwrap()
                        .contains(rustix::io::FdFlags::CLOEXEC)
                );
            }
        }

        #[test]
        fn successful_acquisition_preserves_existing_lock_bytes() {
            let (_root, path) = fixture();
            existing(&path);
            let lock = StartupLock::acquire(path.clone()).unwrap();
            assert_eq!(fs::read(&path).unwrap(), b"retained diagnostic\n");
            drop(lock);
            StartupLock::acquire(path).unwrap();
        }

        #[test]
        fn replacement_between_open_and_lock_preserves_both_files() {
            let (root, path) = fixture();
            existing(&path);
            let prepared = StartupLock::prepare(path.clone()).unwrap();
            let retained = root.path().join("retained.lock");
            fs::rename(&path, &retained).unwrap();
            existing(&path);
            let replacement = identity(&fs::metadata(&path).unwrap());
            assert!(prepared.finish().is_err());
            assert_eq!(fs::read(&path).unwrap(), b"retained diagnostic\n");
            assert_eq!(fs::read(&retained).unwrap(), b"retained diagnostic\n");
            assert_eq!(identity(&fs::metadata(&path).unwrap()), replacement);
        }

        #[test]
        fn replacement_after_acquisition_is_not_unlinked_on_drop() {
            let (root, path) = fixture();
            let lock = StartupLock::acquire(path.clone()).unwrap();
            fs::rename(&path, root.path().join("retained.lock")).unwrap();
            existing(&path);
            assert!(lock.validate().is_err());
            assert!(lock.validate_current().is_err());
            assert!(lock.identity().is_err());
            drop(lock);
            assert_eq!(fs::read(&path).unwrap(), b"retained diagnostic\n");
        }

        #[test]
        fn replaced_parent_or_changed_permissions_fail_before_diagnostic_write() {
            for replacement in [false, true] {
                let (root, _) = fixture();
                let parent = root.path().join("parent");
                fs::create_dir(&parent).unwrap();
                fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
                let path = parent.join("daemon.lock");
                existing(&path);
                let prepared = StartupLock::prepare(path.clone()).unwrap();
                if replacement {
                    fs::rename(&parent, root.path().join("old-parent")).unwrap();
                    fs::create_dir(&parent).unwrap();
                    existing(&path);
                } else {
                    fs::set_permissions(&parent, fs::Permissions::from_mode(0o777)).unwrap();
                }
                assert!(prepared.finish().is_err());
                assert_eq!(fs::read(&path).unwrap(), b"retained diagnostic\n");
            }
        }

        #[test]
        fn symlink_hardlink_fifo_and_bad_modes_are_rejected_without_modification() {
            for kind in [
                "symlink", "dangling", "hardlink", "fifo", "0644", "0400", "setuid",
            ] {
                let (root, path) = fixture();
                let target = root.path().join("target");
                existing(&target);
                match kind {
                    "symlink" => symlink(&target, &path).unwrap(),
                    "dangling" => symlink(root.path().join("missing"), &path).unwrap(),
                    "hardlink" => fs::hard_link(&target, &path).unwrap(),
                    // rustix mkfifoat is not exposed on Apple targets. This
                    // fixture-only command creates exactly the owned path.
                    "fifo" => assert!(
                        std::process::Command::new("/usr/bin/mkfifo")
                            .arg("-m")
                            .arg("600")
                            .arg(&path)
                            .status()
                            .unwrap()
                            .success()
                    ),
                    _ => {
                        existing(&path);
                        let mode = match kind {
                            "0644" => 0o644,
                            "0400" => 0o400,
                            _ => 0o4600,
                        };
                        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
                    }
                }
                let before = fs::symlink_metadata(&path).unwrap();
                let started = std::time::Instant::now();
                assert!(StartupLock::acquire(path.clone()).is_err(), "{kind}");
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(2),
                    "{kind}"
                );
                let after = fs::symlink_metadata(&path).unwrap();
                assert_eq!(identity(&before), identity(&after), "{kind}");
                assert_eq!(before.mode(), after.mode(), "{kind}");
                assert_eq!(fs::read(&target).unwrap(), b"retained diagnostic\n");
                if after.is_file() {
                    assert_eq!(fs::read(&path).unwrap(), b"retained diagnostic\n");
                }
            }
        }

        #[test]
        fn writable_parent_is_not_chmodded_or_populated() {
            let (root, path) = fixture();
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777)).unwrap();
            assert!(StartupLock::acquire(path.clone()).is_err());
            assert!(!path.exists());
            assert_eq!(fs::metadata(root.path()).unwrap().mode() & 0o777, 0o777);
        }

        #[test]
        fn relative_parent_and_owned_0755_directory_remain_supported() {
            let root = tempfile::tempdir_in(".").unwrap();
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
            // tempfile normalizes its returned path even when given ".".
            // Select this exact test-owned child relative to the unchanged cwd.
            let path = PathBuf::from(root.path().file_name().unwrap()).join("daemon.lock");
            assert!(!path.is_absolute());
            let lock = StartupLock::acquire(path.clone()).unwrap();
            assert_eq!(lock.path(), path);
            assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, 0o600);
            assert_eq!(fs::metadata(root.path()).unwrap().mode() & 0o777, 0o755);
        }

        #[test]
        fn parent_traversal_is_rejected_before_creating_lock() {
            let (root, path) = fixture();
            let child = root.path().join("child");
            fs::create_dir(&child).unwrap();
            assert!(StartupLock::acquire(child.join("..").join("daemon.lock")).is_err());
            assert!(!path.exists());
        }
    }
}

#[cfg(unix)]
pub(crate) use platform::StartupLock;

/// Native non-Unix ownership primitives must be implemented before startup can
/// safely provide this authority on that host. Do not fall back to path unlink.
#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) struct StartupLock {
    path: PathBuf,
}

#[cfg(not(unix))]
impl StartupLock {
    pub(crate) fn acquire(path: PathBuf) -> Result<Self, RuntimedError> {
        Err(RuntimedError::AcquireStartupLock {
            path,
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native startup lock ownership is not implemented on this host",
            ),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn validate_current(&self) -> Result<(), RuntimedError> {
        Err(RuntimedError::AcquireStartupLock {
            path: self.path.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native startup lock ownership is not implemented on this host",
            ),
        })
    }

    pub(crate) fn identity(&self) -> Result<(u64, u64), RuntimedError> {
        self.validate_current()?;
        Err(RuntimedError::AcquireStartupLock {
            path: self.path.clone(),
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native startup lock ownership is not implemented on this host",
            ),
        })
    }
}
