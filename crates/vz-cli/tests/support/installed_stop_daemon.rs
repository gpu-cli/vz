//! Explicit external-daemon mode for black-box Stop control-plane checks.
//! This exercises installed executables, not live Machine teardown.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::Fixture;

pub fn installed_binaries() -> Option<(PathBuf, PathBuf)> {
    let cli = std::env::var_os("VZ_TEST_INSTALLED_CLI");
    let daemon = std::env::var_os("VZ_TEST_INSTALLED_DAEMON");
    match (cli, daemon) {
        (None, None) => None,
        (Some(cli), Some(daemon)) => {
            let cli = PathBuf::from(cli);
            let daemon = PathBuf::from(daemon);
            for path in [&cli, &daemon] {
                assert!(path.is_absolute(), "installed binary path must be absolute");
                assert!(
                    fs::symlink_metadata(path).unwrap().is_file(),
                    "installed binary must be a regular file, not a symlink"
                );
            }
            Some((cli, daemon))
        }
        _ => panic!("VZ_TEST_INSTALLED_CLI and VZ_TEST_INSTALLED_DAEMON must both be set"),
    }
}

pub struct ExternalDaemon {
    child: Option<Child>,
    log_path: PathBuf,
    evidence_path: PathBuf,
}

impl ExternalDaemon {
    pub async fn start(fixture: &Fixture, binary: &Path) -> Self {
        let child = Command::new(binary)
            .arg("--state-store-path")
            .arg(&fixture.database)
            .arg("--runtime-data-dir")
            .arg(fixture.root.path().join("runtime"))
            .arg("--socket-path")
            .arg(&fixture.socket)
            .current_dir(fixture.root.path())
            .env_remove("RUST_LOG")
            .env_remove("VZ_RUNTIMED_MIGRATE_LEGACY_CHECKPOINT_ARTIFACTS")
            .env_remove("VZ_SANDBOX_DEFAULT_BASE_IMAGE")
            .env_remove("VZ_SANDBOX_DEFAULT_MAIN_CONTAINER")
            .env_remove("VZ_SANDBOX_DISABLE_LEGACY_DEFAULT_BASE_IMAGE")
            .env_remove("VZ_TEST_INSTALLED_CLI")
            .env_remove("VZ_TEST_INSTALLED_DAEMON")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Self {
            child: Some(child),
            log_path: fixture.socket.with_extension("log"),
            evidence_path: binary.parent().unwrap().join(format!(
                "installed-stop-{}.log",
                fixture.root.path().file_name().unwrap().to_string_lossy()
            )),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if tokio::net::UnixStream::connect(&fixture.socket)
                .await
                .is_ok()
            {
                return server;
            }
            assert!(
                server.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "installed daemon exited before readiness: {}",
                server.diagnostics()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "installed daemon readiness deadline: {}",
            server.diagnostics()
        );
    }

    fn diagnostics(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_else(|error| error.to_string())
    }

    fn preserve_log(&self) -> std::io::Result<()> {
        let log = fs::read(&self.log_path)?;
        let mut evidence = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.evidence_path)?;
        evidence.write_all(&log)?;
        evidence.sync_all()
    }

    pub async fn shutdown(mut self) {
        let child = self.child.as_mut().unwrap();
        assert!(child.try_wait().unwrap().is_none(), "daemon exited early");
        assert!(
            Command::new("/bin/kill")
                .args(["-TERM", &child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                self.child.take();
                self.preserve_log().unwrap();
                assert!(
                    status.success(),
                    "daemon shutdown failed: {}",
                    self.diagnostics()
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("daemon shutdown deadline: {}", self.diagnostics());
    }
}

impl Drop for ExternalDaemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
            let _ = self.preserve_log();
        }
    }
}
