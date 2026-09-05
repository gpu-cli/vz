//! A server binds a fresh name and releases only that same socket inode.

use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;

pub(super) struct BoundSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

pub(super) fn bind(path: &Path) -> io::Result<(UnixListener, BoundSocket)> {
    // bind itself provides the atomic no-adoption check, including dangling
    // symlinks and stale sockets. Never unlink a pathname to make binding work.
    let listener = UnixListener::bind(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        return Err(io::Error::other("bound runtime socket path was replaced"));
    }
    let guard = BoundSocket {
        path: path.into(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok((listener, guard))
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
            && let Err(error) = std::fs::remove_file(&self.path)
        {
            tracing::warn!(path = %self.path.display(), %error, "could not release owned runtime socket");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn existing_socket_file_and_dangling_symlink_are_never_adopted() {
        let temp = tempfile::tempdir().expect("temporary root");
        let path = temp.path().join("runtime.sock");
        std::fs::write(&path, b"foreign").expect("decoy");
        assert!(bind(&path).is_err());
        assert_eq!(std::fs::read(&path).expect("retained"), b"foreign");
        std::fs::remove_file(&path).expect("remove test decoy");
        std::os::unix::fs::symlink(temp.path().join("missing"), &path).expect("link");
        assert!(bind(&path).is_err());
        assert!(
            std::fs::symlink_metadata(&path)
                .expect("retained link")
                .is_symlink()
        );
        std::fs::remove_file(&path).expect("remove test link");
        let (listener, guard) = bind(&path).expect("fresh bind");
        assert!(bind(&path).is_err());
        drop(listener);
        drop(guard);
        assert!(!path.exists());
        let (_listener, _guard) = bind(&path).expect("own graceful cleanup permits fresh bind");
    }
    #[tokio::test]
    async fn cleanup_preserves_replacement_inode() {
        let temp = tempfile::tempdir().expect("temporary root");
        let path = temp.path().join("runtime.sock");
        let (listener, guard) = bind(&path).expect("owned bind");
        std::fs::remove_file(&path).expect("simulate external replacement");
        let replacement = UnixListener::bind(&path).expect("foreign socket");
        let inode = std::fs::symlink_metadata(&path).expect("replacement").ino();
        drop(listener);
        drop(guard);
        assert_eq!(
            std::fs::symlink_metadata(&path)
                .expect("foreign retained")
                .ino(),
            inode
        );
        drop(replacement);
    }
}
