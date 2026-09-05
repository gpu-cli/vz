#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;
use vz_cli::legacy_cli::{LEGACY_COMMAND_REMOVED_CODE, LEGACY_COMMAND_REMOVED_EXIT_CODE};

const STACK_LEAVES: &[&str] = &[
    "up",
    "down",
    "ps",
    "ls",
    "config",
    "events",
    "logs",
    "exec",
    "run",
    "stop",
    "start",
    "restart",
    "dashboard",
];
const EXPECTED_STACK_REJECTION: &str = concat!(
    "{\"error\":{\"code\":\"legacy_command_removed\",",
    "\"command\":\"stack\",",
    "\"message\":\"`vz stack` was removed from the 0.4 public CLI\",",
    "\"migration\":\"Declare services and Machines in vz.json. The 0.4 public CLI is converging on five lifecycle verbs: vz up, vz exec, vz status, vz stop, and vz delete.\",",
    "\"typed_api_migration\":\"Use the topology-scoped typed API for operations outside the five lifecycle verbs.\"}}\n"
);
const EXPECTED_BARE_FLAG_MIGRATION: &str = "The implicit sandbox mode was removed. Declare Developer Environment configuration in vz.json. The 0.4 public CLI is converging on explicit vz up, vz exec, vz status, vz stop, and vz delete lifecycle verbs.";
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
        let project_sentinel = project.join("sentinel");
        fs::write(&project_sentinel, "unchanged").unwrap();
        Self {
            state_db: root.path().join("state/stack-state.db"),
            runtime_dir: root.path().join("runtime"),
            socket: root.path().join("runtime/runtimed.sock"),
            root,
            project,
            project_sentinel,
        }
    }

    fn run(&self, args: &[&str]) -> std::process::Output {
        Command::new(cli_binary())
            .args(args)
            .current_dir(&self.project)
            .env("VZ_RUNTIME_STATE_DB", &self.state_db)
            .env("VZ_RUNTIME_DATA_DIR", &self.runtime_dir)
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            .output()
            .unwrap()
    }

    fn assert_no_runtime_or_state(&self) {
        assert!(
            !self.state_db.exists(),
            "state database must not be created"
        );
        assert!(
            !self.runtime_dir.exists(),
            "runtime directory must not be created"
        );
        assert!(!self.socket.exists(), "daemon socket must not be created");
        assert_eq!(
            fs::read_to_string(&self.project_sentinel).unwrap(),
            "unchanged"
        );
        assert_eq!(fs::read_dir(&self.project).unwrap().count(), 1);
        assert_eq!(fs::read_dir(self.root.path()).unwrap().count(), 1);
        assert!(self.root.path().exists());
    }
}

fn assert_stack_rejected(args: &[&str]) {
    let invocation = IsolatedInvocation::new();
    let output = invocation.run(args);
    assert_eq!(output.status.code(), Some(LEGACY_COMMAND_REMOVED_EXIT_CODE));
    assert!(
        output.stdout.is_empty(),
        "removed command must not write stdout"
    );

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr, EXPECTED_STACK_REJECTION);
    let payload: Value = serde_json::from_str(stderr.trim_end()).unwrap();
    assert_eq!(payload["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
    assert_eq!(payload["error"]["command"], "stack");
    assert!(
        payload["error"]["migration"]
            .as_str()
            .unwrap()
            .contains("vz up")
    );
    assert!(
        payload["error"]["typed_api_migration"]
            .as_str()
            .unwrap()
            .contains("topology-scoped typed API")
    );
    invocation.assert_no_runtime_or_state();
}

fn assert_bare_flag_rejected(args: &[&str], expected_flag: &str) {
    let invocation = IsolatedInvocation::new();
    let output = invocation.run(args);
    assert_eq!(output.status.code(), Some(LEGACY_COMMAND_REMOVED_EXIT_CODE));
    assert!(
        output.stdout.is_empty(),
        "removed flag must not write stdout"
    );

    let stderr = String::from_utf8(output.stderr).unwrap();
    let payload: Value = serde_json::from_str(stderr.trim_end()).unwrap();
    assert_eq!(payload["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
    assert_eq!(payload["error"]["command"], expected_flag);
    assert_eq!(
        payload["error"]["message"],
        format!("`vz {expected_flag}` was removed from the 0.4 public CLI")
    );
    assert_eq!(payload["error"]["migration"], EXPECTED_BARE_FLAG_MIGRATION);
    assert_eq!(
        payload["error"]["typed_api_migration"],
        "Use the topology-scoped typed API for operations outside the five lifecycle verbs."
    );
    assert_eq!(stderr.lines().count(), 1);
    invocation.assert_no_runtime_or_state();
}

#[test]
fn root_help_has_no_stack_command_or_hidden_compatibility_alias() {
    for args in [&["--help"][..], &["help"][..]] {
        let invocation = IsolatedInvocation::new();
        let output = invocation.run(args);
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            !stdout
                .lines()
                .any(|line| line.trim_start().starts_with("stack"))
        );
        for flag in [
            "--continue",
            "--resume",
            "--name",
            "--ephemeral",
            "--cpus",
            "--memory",
            "--base-image",
            "--main-container",
            "--control-plane",
        ] {
            assert!(!stdout.contains(flag), "root help exposed removed {flag}");
        }
        invocation.assert_no_runtime_or_state();
    }
}

#[test]
fn every_removed_bare_mode_flag_is_structured_and_nonmutating() {
    for (args, expected_flag) in [
        (&["-c"][..], "-c"),
        (&["-hc"][..], "-c"),
        (&["-vc"][..], "-c"),
        (&["-qc"][..], "-c"),
        (&["--continue"][..], "--continue"),
        (&["-r", "target"][..], "-r"),
        (&["-vrtarget"][..], "-r"),
        (&["-vrcandidate"][..], "-r"),
        (&["-Vcr"][..], "-c"),
        (&["--resume=target"][..], "--resume"),
        (&["--name", "target"][..], "--name"),
        (&["--ephemeral"][..], "--ephemeral"),
        (&["--cpus", "4"][..], "--cpus"),
        (&["--memory=4096"][..], "--memory"),
        (&["--base-image", "alpine"][..], "--base-image"),
        (&["--main-container=app"][..], "--main-container"),
        (&["--control-plane", "daemon-grpc"][..], "--control-plane"),
        (&["help", "--name", "stack"][..], "--name"),
    ] {
        assert_bare_flag_rejected(args, expected_flag);
    }
}

#[test]
fn removed_stack_root_help_and_unknown_nested_arguments_are_rejected() {
    assert_stack_rejected(&["stack"]);
    assert_stack_rejected(&["stack", "--help"]);
    assert_stack_rejected(&["stack", "unknown", "--arbitrary", "value"]);
    assert_stack_rejected(&["--json", "stack", "--help"]);
    assert_stack_rejected(&["--", "stack"]);
}

#[test]
fn generated_help_traversal_cannot_revive_the_removed_stack_parser() {
    assert_stack_rejected(&["help", "stack"]);
    assert_stack_rejected(&["help", "stack", "up"]);
    assert_stack_rejected(&["help", "stack", "--help"]);
    assert_stack_rejected(&["help", "--", "stack"]);
}

#[test]
fn every_former_stack_leaf_is_rejected_before_state_or_daemon_access() {
    for leaf in STACK_LEAVES {
        assert_stack_rejected(&["stack", leaf, "--help"]);
        assert_stack_rejected(&["stack", leaf, "arbitrary", "--unknown"]);
    }
}
