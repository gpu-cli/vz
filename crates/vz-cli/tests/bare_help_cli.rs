#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

use tempfile::TempDir;

const INSTALLED_CLI_ENV: &str = "VZ_TEST_INSTALLED_CLI";

fn cli_binary() -> PathBuf {
    let Some(path) = std::env::var_os(INSTALLED_CLI_ENV).map(PathBuf::from) else {
        return PathBuf::from(env!("CARGO_BIN_EXE_vz"));
    };
    assert!(path.is_absolute(), "{INSTALLED_CLI_ENV} must be absolute");
    assert!(
        path.is_file(),
        "{INSTALLED_CLI_ENV} must name an existing regular file: {}",
        path.display()
    );
    path
}

struct IsolatedInvocation {
    root: TempDir,
    project: PathBuf,
    project_definition: PathBuf,
    project_sentinel: PathBuf,
    state_db: PathBuf,
    runtime_dir: PathBuf,
    socket: PathBuf,
}

impl IsolatedInvocation {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        fs::create_dir(&project).unwrap();

        let project_definition = project.join("vz.json");
        let project_sentinel = project.join("sentinel");
        fs::write(
            &project_definition,
            b"deliberately invalid project definition\n",
        )
        .unwrap();
        fs::write(&project_sentinel, b"project unchanged\n").unwrap();

        Self {
            state_db: root.path().join("state/stack-state.db"),
            runtime_dir: root.path().join("runtime"),
            socket: root.path().join("runtime/runtimed.sock"),
            root,
            project,
            project_definition,
            project_sentinel,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(cli_binary())
            .args(args)
            .current_dir(&self.project)
            .env("VZ_RUNTIME_STATE_DB", &self.state_db)
            .env("VZ_RUNTIME_DATA_DIR", &self.runtime_dir)
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "definitely-not-a-boolean")
            .env("VZ_CONTROL_PLANE_TRANSPORT", "definitely-not-a-transport")
            .env_remove("RUST_LOG")
            .output()
            .unwrap()
    }

    fn assert_unchanged(&self) {
        assert_eq!(
            fs::read(&self.project_definition).unwrap(),
            b"deliberately invalid project definition\n"
        );
        assert_eq!(
            fs::read(&self.project_sentinel).unwrap(),
            b"project unchanged\n"
        );
        assert_eq!(fs::read_dir(&self.project).unwrap().count(), 2);
        assert_eq!(fs::read_dir(self.root.path()).unwrap().count(), 1);
        assert!(
            !self.state_db.exists(),
            "state database must not be created"
        );
        assert!(
            !self.runtime_dir.exists(),
            "runtime directory must not be created"
        );
        assert!(!self.socket.exists(), "daemon socket must not be created");
    }
}

#[test]
fn bare_invocation_is_exact_static_help_without_state_or_daemon_access() {
    let invocation = IsolatedInvocation::new();

    let bare = invocation.run(&[]);
    assert!(
        bare.status.success(),
        "bare vz failed: {}",
        String::from_utf8_lossy(&bare.stderr)
    );
    assert!(bare.stderr.is_empty(), "bare vz must not write stderr");
    assert!(!bare.stdout.is_empty(), "bare vz must print top-level help");
    invocation.assert_unchanged();

    let explicit_help = invocation.run(&["--help"]);
    assert!(explicit_help.status.success());
    assert!(explicit_help.stderr.is_empty());
    assert_eq!(
        bare.stdout, explicit_help.stdout,
        "bare vz must exactly match static top-level --help"
    );
    invocation.assert_unchanged();
}

#[test]
fn read_only_globals_without_a_command_are_the_same_nonmutating_help() {
    for args in [
        &["--json"][..],
        &["--quiet"][..],
        &["-v"][..],
        &["-vvq"][..],
    ] {
        let invocation = IsolatedInvocation::new();
        let explicit_help = invocation.run(&["--help"]);
        assert!(explicit_help.status.success());
        assert!(explicit_help.stderr.is_empty());
        let output = invocation.run(args);

        assert!(
            output.status.success(),
            "read-only-global invocation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        assert_eq!(
            output.stdout, explicit_help.stdout,
            "read-only globals without a command must print exact top-level help"
        );
        invocation.assert_unchanged();
    }
}
