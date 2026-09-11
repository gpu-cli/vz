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

/// Free bytes on the volume holding `path`.
///
/// This is the only honest way to measure what a copy-on-write clone costs, and
/// the reason it exists as a public function rather than as test scaffolding.
/// APFS reports a clone's `st_blocks` as the *full* logical allocation, because
/// both inodes reference the same blocks: measured on a real 80 GiB Machine
/// template disk with 32.9 GiB allocated, the clone's `st_blocks` matched the
/// parent's 32.9 GiB exactly while the volume's free space fell by 28 KB. A
/// check that compares per-file allocated size therefore reads a perfect clone
/// as a deep copy. Free-space delta is the observable that tells them apart.
pub fn volume_free_bytes(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let raw_path = CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `statvfs` fills a caller-owned, fully initialised POD struct and
    // retains neither the pointer nor the path string past the call. The path is
    // a live NUL-terminated C string for its duration. Reading the struct is
    // sound whether or not the call succeeded, because it was zero-initialised
    // before the call; the return value decides whether it is meaningful.
    #[allow(unsafe_code)]
    let (result, stats) = unsafe {
        let mut stats: libc::statvfs = std::mem::zeroed();
        let result = libc::statvfs(raw_path.as_ptr(), &raw mut stats);
        (result, stats)
    };
    ensure!(
        result == 0,
        "could not read free space for {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    Ok(stats.f_frsize.saturating_mul(stats.f_bavail as u64))
}

/// Device offset of the physical extent backing `path` at `offset`.
///
/// The direct observation of copy-on-write, where [`volume_free_bytes`] is an
/// indirect one. Two files that share blocks report the same offset; a file
/// holding its own copy of the bytes reports a different one.
///
/// Both exist because they answer the same question under different
/// constraints. Free space is what a gate lane measures, because it is what a
/// clone actually *costs* and the lane owns its volume. But free space is a
/// property of the whole volume, so it cannot be asserted anywhere something
/// else may be writing -- measured 2026-09-10, a bound on it passed run alone
/// and failed inside a parallel test suite whose neighbours moved more bytes
/// during the window than the file under test. This function is unaffected by
/// anything outside the two files.
pub fn physical_extent_at(path: &Path, offset: u64) -> Result<u64> {
    use std::os::unix::io::AsRawFd;

    // `struct log2phys` from <sys/fcntl.h>. `repr(C)` reproduces the padding
    // after the 32-bit flags that the kernel's own layout has.
    #[repr(C)]
    struct Log2Phys {
        flags: u32,
        contigbytes: i64,
        devoffset: i64,
    }
    const F_LOG2PHYS_EXT: i32 = 65;

    let file = std::fs::File::open(path)?;
    // F_LOG2PHYS_EXT takes the logical offset as INPUT in the same field it
    // returns the device offset in, unlike plain F_LOG2PHYS which reads the
    // file position. Asking only about offset 0 would ask about the one region a
    // forked disk is guaranteed to have rewritten -- its superblock and journal.
    // `contigbytes` is the number of bytes to map, and it must be a real
    // request. Passing 0 is not "map the default": the kernel has nothing to
    // resolve, and the `devoffset` it leaves behind is not a dependable answer
    // -- two distinct, fully allocated files could report the SAME offset,
    // which made the caller's clone-versus-copy control trip its own vacuity
    // guard intermittently ("a streamed byte copy shares its source's blocks;
    // this measurement cannot tell a clone from a copy and proves nothing").
    // One block is the smallest request that names a single extent.
    const MAP_BYTES: i64 = 4096;
    let mut mapping = Log2Phys {
        flags: 0,
        contigbytes: MAP_BYTES,
        devoffset: offset as i64,
    };
    // SAFETY: F_LOG2PHYS_EXT takes a pointer to one `struct log2phys`, which
    // `mapping` is: correctly sized, aligned, fully initialised, and exclusively
    // borrowed for the duration of the call. `file` owns an open descriptor that
    // outlives the call, and the kernel writes only into that struct, retaining
    // neither pointer afterwards.
    #[allow(unsafe_code)]
    let result = unsafe {
        libc::fcntl(
            file.as_raw_fd(),
            F_LOG2PHYS_EXT,
            &raw mut mapping as *mut libc::c_void,
        )
    };
    ensure!(
        result == 0,
        "could not read the physical extent of {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    Ok(mapping.devoffset as u64)
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

    /// The load-bearing measurement: cloning a directory tree costs metadata,
    /// not bytes — and the naive way of checking that would say the opposite.
    ///
    /// This is the property Machine forking rests on. If a fork deep-copied the
    /// disk it would cost as much as a cold boot and the whole feature would be
    /// pointless, so the claim is measured rather than assumed. It is measured
    /// as **volume free space**, because APFS reports both inodes as fully
    /// allocated: the second assertion below deliberately proves that the
    /// per-file check fails, so nobody "fixes" this test into the wrong one.
    #[test]
    fn cloning_a_tree_costs_free_space_metadata_not_bytes() {
        use std::os::unix::fs::MetadataExt;

        // Large enough that a deep copy is unmistakable against ambient noise,
        // small enough to stay a unit test. The clone itself takes tens of
        // milliseconds, so the measurement window is far too short for other
        // processes to move free space by anything near this much.
        const TREE_BYTES: u64 = 128 * 1024 * 1024;
        const FILES: u64 = 8;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("machine-store");
        std::fs::create_dir_all(source.join("data/docker-machines")).unwrap();
        // Incompressible, so no filesystem-level compression can make a deep
        // copy look cheap.
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut block = vec![0_u8; 1024 * 1024];
        for index in 0..FILES {
            for chunk in block.chunks_mut(8) {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                chunk.copy_from_slice(&seed.to_le_bytes()[..chunk.len()]);
            }
            let path = source.join(format!("data/docker-machines/data-{index}.img"));
            let mut file = std::fs::File::create(&path).unwrap();
            for _ in 0..(TREE_BYTES / FILES / block.len() as u64) {
                std::io::Write::write_all(&mut file, &block).unwrap();
            }
            file.sync_all().unwrap();
        }

        let allocated: u64 = (0..FILES)
            .map(|index| {
                std::fs::metadata(source.join(format!("data/docker-machines/data-{index}.img")))
                    .unwrap()
                    .blocks()
                    * 512
            })
            .sum();
        assert!(
            allocated >= TREE_BYTES,
            "fixture must actually occupy its bytes: {allocated}"
        );

        let destination = temp.path().join("forked-store");
        let free_before = volume_free_bytes(temp.path()).unwrap();
        let started = std::time::Instant::now();
        clone_path(&source, &destination).unwrap();
        let elapsed = started.elapsed();
        let free_after = volume_free_bytes(temp.path()).unwrap();
        let consumed = free_before.saturating_sub(free_after);

        // A deep copy would consume the whole tree. A clone consumes metadata.
        assert!(
            consumed < TREE_BYTES / 8,
            "clone of {TREE_BYTES} bytes consumed {consumed} bytes of free space in {elapsed:?}; \
             that is a deep copy, not a copy-on-write clone"
        );

        // And here is why the obvious check is the wrong one: both inodes report
        // the same allocated size, because they reference the same blocks. A
        // check written this way would fail on a perfect clone.
        let cloned_allocated: u64 = (0..FILES)
            .map(|index| {
                std::fs::metadata(
                    destination.join(format!("data/docker-machines/data-{index}.img")),
                )
                .unwrap()
                .blocks()
                    * 512
            })
            .sum();
        assert_eq!(
            cloned_allocated, allocated,
            "APFS reports a clone as fully allocated; if this ever stops being \
             true the free-space measurement above is still the correct one"
        );

        // Separate inodes, so a fork writing into its disk cannot reach its
        // parent's. This is what makes a fork an isolation boundary rather than
        // a shared view.
        let first = source.join("data/docker-machines/data-0.img");
        let cloned_first = destination.join("data/docker-machines/data-0.img");
        std::fs::write(&cloned_first, b"diverged").unwrap();
        assert_eq!(std::fs::metadata(&first).unwrap().len(), TREE_BYTES / FILES);
        assert_eq!(std::fs::metadata(&cloned_first).unwrap().len(), 8);
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
