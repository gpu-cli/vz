//! Copy-on-write cloning of a host file or directory tree.
//!
//! One wrapper over `clonefile(2)`, used for both shapes it supports. Before
//! this module the repo had a private single-file `clone_file` inside template
//! publication, and the belief grew from it that "there is no directory-tree
//! copy primitive". There always was: `clonefile` with a directory source
//! clones the hierarchy recursively — the recursion is the syscall's, not the
//! caller's — and preserves symlinks as symlinks, so no entry is ever followed
//! out of the tree during the copy.
//!
//! Copy-on-write means a clone of a large tree costs metadata rather than
//! bytes, and a writer into the clone allocates only the blocks it touches. The
//! clone is a separate inode, never a hard link, so writes never reach the
//! source.
//!
//! There is deliberately no full-copy fallback. A caller that asked for a
//! snapshot of a multi-gibibyte worktree on every boot and silently got a deep
//! copy would have a different feature wearing this one's name, so a non-APFS
//! destination fails with the OS error instead.

use std::path::Path;

use anyhow::{Result, ensure};

/// Clone `source` to `destination` copy-on-write.
///
/// `source` may be a regular file or a directory; a directory is cloned
/// recursively. `destination` must not exist — `clonefile` enforces that with
/// `EEXIST`, and cloning over an existing tree would silently merge two copies.
pub fn clone_path(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let raw_source = CString::new(source.as_os_str().as_bytes())?;
        let raw_destination = CString::new(destination.as_os_str().as_bytes())?;
        // SAFETY: both paths are live NUL-terminated C strings for the duration
        // of the call, and neither is retained by the kernel afterwards.
        // `clonefile` creates a separate copy-on-write inode at the
        // destination and never a hard link to the source, so a writer into the
        // clone cannot reach what it was cloned from.
        #[allow(unsafe_code)]
        let result = unsafe { libc::clonefile(raw_source.as_ptr(), raw_destination.as_ptr(), 0) };
        ensure!(
            result == 0,
            "APFS clone failed (no full-copy fallback): {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (source, destination);
        anyhow::bail!("copy-on-write cloning requires an APFS host")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn a_directory_source_is_cloned_recursively_with_symlinks_preserved() {
        // This is the test the "no directory-tree copy primitive" belief needed.
        // Nothing here walks the tree: one call clones every level.
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("src");
        std::fs::create_dir_all(source.join("nested/deeper")).unwrap();
        std::fs::write(source.join("nested/deeper/file"), b"payload").unwrap();
        std::os::unix::fs::symlink("nested/deeper/file", source.join("link")).unwrap();

        let destination = temp.path().join("clone");
        clone_path(&source, &destination).unwrap();

        assert_eq!(
            std::fs::read(destination.join("nested/deeper/file")).unwrap(),
            b"payload"
        );
        // A symlink cloned as a symlink, not as the file it points at: the copy
        // never followed a link out of the tree.
        assert!(
            std::fs::symlink_metadata(destination.join("link"))
                .unwrap()
                .file_type()
                .is_symlink()
        );

        // Separate inodes: a write into the clone must not reach the source.
        // This is the property the whole snapshot mode rests on.
        std::fs::write(destination.join("nested/deeper/file"), b"changed").unwrap();
        assert_eq!(
            std::fs::read(source.join("nested/deeper/file")).unwrap(),
            b"payload"
        );
    }

    #[test]
    fn a_regular_file_source_is_cloned() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("one");
        std::fs::write(&source, b"bytes").unwrap();
        let destination = temp.path().join("two");
        clone_path(&source, &destination).unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"bytes");
    }

    #[test]
    fn an_existing_destination_is_refused_rather_than_merged() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("src");
        std::fs::create_dir_all(&source).unwrap();
        let destination = temp.path().join("dst");
        std::fs::create_dir_all(&destination).unwrap();
        let error = clone_path(&source, &destination).unwrap_err().to_string();
        assert!(error.contains("APFS clone failed"), "{error}");
    }
}
