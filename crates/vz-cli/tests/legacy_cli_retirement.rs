#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use vz_cli::legacy_cli::{
    LEGACY_COMMAND_REMOVED_CODE, LEGACY_COMMAND_REMOVED_EXIT_CODE, REMOVED_ROOT_COMMANDS,
};

const INVENTORY: &str = include_str!("../../../config/cli-removal-v0.4.json");
const EXPECTED_ROOT_MIGRATION: &str = "Declare Developer Environment topology in vz.json. Use vz status to inspect it, vz exec for Machine execution, and vz stop to preserve it. The complete 0.4 lifecycle adds vz up and vz delete; consult installed help for implemented DEV capabilities.";
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
        #[cfg(target_os = "macos")]
        let root = tempfile::tempdir_in("/private/tmp").unwrap();
        #[cfg(not(target_os = "macos"))]
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        fs::create_dir(&project).unwrap();
        let project_sentinel = project.join("sentinel");
        fs::write(&project_sentinel, "unchanged").unwrap();
        fs::write(
            project.join("vz.json"),
            "invalid definition: must not be read",
        )
        .unwrap();
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
        let mut child = Command::new(cli_binary())
            .args(args)
            .current_dir(&self.project)
            .env("VZ_RUNTIME_STATE_DB", &self.state_db)
            .env("VZ_RUNTIME_DATA_DIR", &self.runtime_dir)
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            .env(
                "CARGO_BIN_EXE_vz-runtimed",
                self.root.path().join("absent-daemon"),
            )
            .env("VZ_ENVIRONMENT_ID", "invalid-selector-must-not-be-read")
            .env("VZ_MACHINE_ID", "invalid-selector-must-not-be-read")
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                return child.wait_with_output().unwrap();
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!("static rejection/help exceeded deadline for {args:?}: {output:?}");
            }
            std::thread::sleep(Duration::from_millis(2));
        }
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
        assert_eq!(
            fs::read_to_string(self.project.join("vz.json")).unwrap(),
            "invalid definition: must not be read"
        );
        assert_eq!(fs::read_dir(&self.project).unwrap().count(), 2);
        assert_eq!(fs::read_dir(self.root.path()).unwrap().count(), 1);
        assert!(self.root.path().exists());
    }
}

fn assert_root_rejected(args: &[&str], root: &str) {
    let invocation = IsolatedInvocation::new();
    let output = invocation.run(args);
    assert_eq!(output.status.code(), Some(LEGACY_COMMAND_REMOVED_EXIT_CODE));
    assert!(
        output.stdout.is_empty(),
        "removed command must not write stdout"
    );

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1);
    let payload: Value = serde_json::from_str(stderr.trim_end()).unwrap();
    assert_eq!(payload["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
    assert_eq!(payload["error"]["command"], root);
    assert_eq!(payload["error"]["migration"], EXPECTED_ROOT_MIGRATION);
    assert_eq!(
        payload["error"]["message"],
        format!("`vz {root}` was removed from the 0.4 public CLI")
    );
    assert_eq!(payload.as_object().unwrap().len(), 1);
    assert_eq!(payload["error"].as_object().unwrap().len(), 5);
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
fn root_help_has_no_retired_command_or_hidden_compatibility_alias() {
    for args in [&["--help"][..], &["help"][..]] {
        let invocation = IsolatedInvocation::new();
        let output = invocation.run(args);
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        for root in REMOVED_ROOT_COMMANDS {
            assert!(
                !stdout
                    .lines()
                    .any(|line| line.trim_start().starts_with(&format!("{root} ")))
            );
        }
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
fn every_removed_root_and_help_route_rejects_without_state_or_daemon_access() {
    for root in REMOVED_ROOT_COMMANDS {
        for args in [
            vec![*root],
            vec![*root, "--help"],
            vec!["help", *root],
            vec!["--json", *root, "--help"],
            vec!["--", *root],
            vec!["--help", *root],
            vec!["--version", *root],
            vec!["help", "--", *root],
            vec!["-vvq", *root, "unknown"],
        ] {
            assert_root_rejected(&args, root);
        }
    }
}

#[test]
fn every_inventoried_nested_path_is_rejected_not_hidden_or_executable() {
    let inventory: Value = serde_json::from_str(INVENTORY).unwrap();
    let paths = inventory["dev_baseline"]["help_paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| &entry["path"])
        .chain(inventory["normative_only_paths"].as_array().unwrap().iter());
    for path in paths {
        let path = path
            .as_array()
            .unwrap()
            .iter()
            .map(|part| part.as_str().unwrap())
            .collect::<Vec<_>>();
        let root = path[0];
        for suffix in [&[][..], &["--help"][..], &["arbitrary", "--unknown"][..]] {
            let args = path
                .iter()
                .copied()
                .chain(suffix.iter().copied())
                .collect::<Vec<_>>();
            assert_root_rejected(&args, root);
        }
        let help = std::iter::once("help")
            .chain(path.iter().copied())
            .collect::<Vec<_>>();
        assert_root_rejected(&help, root);
    }
}

#[test]
fn machine_readable_dev_inventory_matches_normative_removal_and_real_help() {
    let inventory: Value = serde_json::from_str(INVENTORY).unwrap();
    assert_eq!(inventory["schema_version"], 1);
    assert_eq!(
        inventory["status"],
        "DEV_TRANSITIONAL_NOT_RELEASE_ACCEPTANCE"
    );
    assert_eq!(
        inventory["removed_roots"],
        serde_json::json!(REMOVED_ROOT_COMMANDS)
    );
    assert_eq!(
        inventory["required_release_roots"],
        serde_json::json!(["up", "exec", "status", "stop", "delete"])
    );
    assert_eq!(inventory["pinned_release_baseline"]["state"], "pending");
    assert!(inventory["pinned_release_baseline"]["artifact_sha256"].is_null());
    assert_eq!(inventory["rejection"]["code"], LEGACY_COMMAND_REMOVED_CODE);
    assert_eq!(
        inventory["rejection"]["exit_code"],
        LEGACY_COMMAND_REMOVED_EXIT_CODE
    );
    let normative = include_str!("../../../planning/developer-environments/legacy-cli-removal.md");
    let blocks = normative
        .split("```text\n")
        .skip(1)
        .map(|rest| rest.split("\n```").next().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(blocks[0].lines().collect::<Vec<_>>(), REMOVED_ROOT_COMMANDS);
    let normative_flags = blocks[1]
        .lines()
        .flat_map(|line| line.split(", "))
        .collect::<Vec<_>>();
    assert_eq!(
        inventory["removed_root_flags"],
        serde_json::json!(normative_flags)
    );
    let mut paths = std::collections::BTreeSet::new();
    for entry in inventory["dev_baseline"]["help_paths"].as_array().unwrap() {
        let path = entry["path"].as_array().unwrap();
        assert!(REMOVED_ROOT_COMMANDS.contains(&path[0].as_str().unwrap()));
        assert!(
            paths.insert(
                path.iter()
                    .map(|part| part.as_str().unwrap())
                    .collect::<Vec<_>>()
            )
        );
        let hash = entry["help_sha256"].as_str().unwrap();
        assert_eq!(hash.len(), 64);
        assert!(
            hash.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
    }
    assert_eq!(paths.len(), 160);
    let stack_paths = std::iter::once(vec!["stack"])
        .chain(
            [
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
            ]
            .into_iter()
            .map(|leaf| vec!["stack", leaf]),
        )
        .collect::<Vec<_>>();
    assert_eq!(
        inventory["normative_only_paths"],
        serde_json::json!(stack_paths)
    );
    assert!(paths.contains(&vec!["vm", "linux", "e2e"]));
    assert!(paths.contains(&vec!["debug", "docker", "build", "cache", "prune"]));
    let invocation = IsolatedInvocation::new();
    let output = invocation.run(&["--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    let commands = help
        .split("Commands:\n")
        .nth(1)
        .unwrap()
        .split("\n\n")
        .next()
        .unwrap();
    let actual = commands
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| *name != "help")
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::json!(actual),
        inventory["implemented_dev_roots"]
    );
    invocation.assert_no_runtime_or_state();
}

#[cfg(unix)]
#[test]
fn retired_roots_never_connect_to_existing_socket_or_change_existing_state() {
    use std::os::unix::net::UnixListener;

    let invocation = IsolatedInvocation::new();
    fs::create_dir(&invocation.runtime_dir).unwrap();
    fs::create_dir(invocation.state_db.parent().unwrap()).unwrap();
    let sentinel = b"not a SQLite database; must remain byte-identical";
    fs::write(&invocation.state_db, sentinel).unwrap();
    let listener = UnixListener::bind(&invocation.socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    for root in REMOVED_ROOT_COMMANDS {
        let output = invocation.run(&[root, "arbitrary", "--unknown"]);
        assert_eq!(output.status.code(), Some(LEGACY_COMMAND_REMOVED_EXIT_CODE));
        assert!(output.stdout.is_empty());
        let payload: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(payload["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
        assert_eq!(payload["error"]["command"], *root);
        match listener.accept() {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
            Ok(_) => panic!("retired root connected to runtime socket: {root}"),
        }
        assert_eq!(fs::read(&invocation.state_db).unwrap(), sentinel);
        assert_eq!(
            fs::read_dir(invocation.state_db.parent().unwrap())
                .unwrap()
                .count(),
            1
        );
        assert_eq!(fs::read_dir(&invocation.runtime_dir).unwrap().count(), 1);
        assert_eq!(
            fs::read_to_string(&invocation.project_sentinel).unwrap(),
            "unchanged"
        );
    }
}
