#![cfg(target_os = "macos")]
#![allow(clippy::expect_used)]

//! Host-only regressions stay outside the exactly-one-test physical driver.
#[path = "support/reaped_helper_socket.rs"]
mod reaped_helper_socket;
use reaped_helper_socket::CapturedHelperSocket;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Child;

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::os::unix::net::UnixListener;
    use std::process::Command;

    struct FixtureChild(Child);
    impl Drop for FixtureChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn child() -> FixtureChild {
        FixtureChild(
            Command::new("/bin/sleep")
                .arg("60")
                .spawn()
                .expect("owned child"),
        )
    }
    fn fixture() -> (tempfile::TempDir, PathBuf, UnixListener) {
        let root = tempfile::Builder::new()
            .prefix("vz-reap-")
            .tempdir_in("/private/tmp")
            .expect("short root");
        let path = root.path().join("control.sock");
        let listener = UnixListener::bind(&path).expect("socket");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
        (root, path, listener)
    }

    #[test]
    fn exact_captured_socket_removed_only_after_positive_sigkill_reap() {
        let (_root, path, _listener) = fixture();
        let mut owned = child();
        let captured = CapturedHelperSocket::capture(&mut owned.0, &path).expect("capture");
        assert!(path.exists());
        let receipt = captured
            .kill_reap_and_remove(&mut owned.0)
            .expect("exact cleanup");
        let receipt = serde_json::to_value(&receipt).expect("receipt");
        assert_eq!(receipt["child_sigkill_reaped"], true);
        assert_eq!(receipt["captured_socket_removed"], true);
        assert_eq!(
            owned
                .0
                .try_wait()
                .expect("status")
                .expect("reaped")
                .signal(),
            Some(9)
        );
        assert!(!path.exists());
    }

    #[test]
    fn foreign_or_already_reaped_child_cannot_authorize_cleanup() {
        let (_root, path, _listener) = fixture();
        let mut owned = child();
        let mut foreign = child();
        let captured = CapturedHelperSocket::capture(&mut owned.0, &path).expect("capture");
        assert!(captured.kill_reap_and_remove(&mut foreign.0).is_err());
        assert!(owned.0.try_wait().expect("owned live").is_none());
        assert!(foreign.0.try_wait().expect("foreign live").is_none());
        let captured = CapturedHelperSocket::capture(&mut owned.0, &path).expect("capture");
        owned.0.kill().expect("kill");
        owned.0.wait().expect("reap");
        assert!(captured.kill_reap_and_remove(&mut owned.0).is_err());
        assert!(CapturedHelperSocket::capture(&mut owned.0, &path).is_err());
        assert!(path.exists());
    }

    #[test]
    fn regular_symlink_and_new_socket_replacements_are_never_unlinked() {
        for kind in ["regular", "symlink", "socket"] {
            let (root, path, _original) = fixture();
            let original_inode = std::fs::symlink_metadata(&path)
                .expect("original socket")
                .ino();
            let mut owned = child();
            let captured = CapturedHelperSocket::capture(&mut owned.0, &path).expect("capture");
            std::fs::remove_file(&path).expect("replace test fixture socket");
            let _replacement = match kind {
                "regular" => {
                    std::fs::write(&path, b"foreign").expect("file");
                    None
                }
                "symlink" => {
                    symlink(root.path().join("missing"), &path).expect("link");
                    None
                }
                _ => {
                    let listener = UnixListener::bind(&path).expect("replacement socket");
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                        .expect("same private socket mode");
                    Some(listener)
                }
            };
            let before = std::fs::symlink_metadata(&path).expect("replacement");
            assert_ne!(before.ino(), original_inode);
            assert!(
                captured.kill_reap_and_remove(&mut owned.0).is_err(),
                "{kind}"
            );
            let after = std::fs::symlink_metadata(&path).expect("preserved replacement");
            assert_eq!(
                (before.dev(), before.ino(), before.mode()),
                (after.dev(), after.ino(), after.mode())
            );
        }
    }

    #[test]
    fn replaced_or_writable_parent_is_never_cleanup_authority() {
        for link in [false, true] {
            let (root, path, _listener) = fixture();
            let parent = root.path().join("control");
            std::fs::create_dir(&parent).expect("parent");
            let moved_path = parent.join("control.sock");
            std::fs::rename(&path, &moved_path).expect("move owned socket");
            let mut owned = child();
            let captured =
                CapturedHelperSocket::capture(&mut owned.0, &moved_path).expect("capture");
            let retained = root.path().join("retained");
            std::fs::rename(&parent, &retained).expect("replace parent");
            if link {
                symlink(&retained, &parent).expect("parent link");
            } else {
                std::fs::create_dir(&parent).expect("new parent");
            }
            assert!(captured.kill_reap_and_remove(&mut owned.0).is_err());
            assert!(retained.join("control.sock").exists());
        }
        let (root, path, _listener) = fixture();
        let mut owned = child();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o777))
            .expect("unsafe parent");
        assert!(CapturedHelperSocket::capture(&mut owned.0, &path).is_err());
        assert!(owned.0.try_wait().expect("still live").is_none());
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
            .expect("restore test root");
    }
}
