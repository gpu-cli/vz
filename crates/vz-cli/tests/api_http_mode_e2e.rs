//! Retired CLI commands must not contact either legacy transport.
//!
//! These replace obsolete tests that expected infrastructure CLI commands to
//! succeed. They are retirement evidence, not typed API/backend certification.
//! The coverage handoff is recorded in fixtures/retired-api-cli-coverage.md.

#![cfg(unix)]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use vz_cli::legacy_cli::{LEGACY_COMMAND_REMOVED_CODE, LEGACY_COMMAND_REMOVED_EXIT_CODE};

const INVENTORY: &str = include_str!("../../../config/cli-removal-v0.4.json");

fn cli_binary() -> PathBuf {
    let Some(path) = std::env::var_os("VZ_TEST_INSTALLED_CLI").map(PathBuf::from) else {
        return PathBuf::from(env!("CARGO_BIN_EXE_vz"));
    };
    assert!(
        path.is_absolute() && path.is_file(),
        "installed CLI must be an absolute regular-file path"
    );
    path
}

fn bounded_output(command: &mut Command) -> Output {
    let mut child = command
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
            panic!("retired command did not finish within static-rejection deadline: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn assert_transport_cannot_restore_retired_commands(transport: &str) {
    #[cfg(target_os = "macos")]
    let root = tempfile::tempdir_in("/private/tmp").unwrap();
    #[cfg(not(target_os = "macos"))]
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir(&project).unwrap();
    let definition = b"invalid project definition: retired commands must not discover it";
    let sentinel = b"project sentinel must remain byte-identical";
    fs::write(project.join("vz.json"), definition).unwrap();
    fs::write(project.join("sentinel"), sentinel).unwrap();
    let state_db = root.path().join("state.db");
    let state_bytes = b"invalid SQLite state: retired commands must not open it";
    fs::write(&state_db, state_bytes).unwrap();
    let socket = root.path().join("daemon.sock");
    let uds = UnixListener::bind(&socket).unwrap();
    uds.set_nonblocking(true).unwrap();
    let http = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    http.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}", http.local_addr().unwrap());
    let runtime_dir = root.path().join("runtime-must-not-exist");

    let inventory: Value = serde_json::from_str(INVENTORY).unwrap();
    let paths = inventory["dev_baseline"]["help_paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| &entry["path"])
        .chain(inventory["normative_only_paths"].as_array().unwrap().iter());
    let mut invocations: Vec<(Vec<&str>, &str)> = Vec::new();
    for path in paths {
        let path = path
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        let root_name = path[0];
        invocations.push((path.clone(), root_name));
        invocations.push((
            path.into_iter().chain(std::iter::once("--help")).collect(),
            root_name,
        ));
    }
    for flag in inventory["removed_root_flags"].as_array().unwrap() {
        let flag = flag.as_str().unwrap();
        invocations.push((vec![flag], flag));
    }
    assert_eq!(invocations.len(), 359);

    for (args, removed) in invocations {
        let output = bounded_output(
            Command::new(cli_binary())
                .args(&args)
                .current_dir(&project)
                .env("VZ_CONTROL_PLANE_TRANSPORT", transport)
                .env("VZ_RUNTIME_API_BASE_URL", &api_url)
                .env("VZ_RUNTIME_STATE_DB", &state_db)
                .env("VZ_RUNTIME_DAEMON_SOCKET", &socket)
                .env("VZ_RUNTIME_DATA_DIR", &runtime_dir)
                .env("VZ_RUNTIME_DAEMON_AUTOSTART", "true")
                .env(
                    "CARGO_BIN_EXE_vz-runtimed",
                    root.path().join("absent-daemon"),
                )
                .env("VZ_ENVIRONMENT_ID", "invalid-selector-must-not-be-read")
                .env("VZ_MACHINE_ID", "invalid-selector-must-not-be-read")
                .env_remove("RUST_LOG"),
        );
        assert_eq!(
            output.status.code(),
            Some(LEGACY_COMMAND_REMOVED_EXIT_CODE),
            "{transport}: {args:?}"
        );
        assert!(output.stdout.is_empty(), "{transport}: {args:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(stderr.lines().count(), 1);
        let payload: Value = serde_json::from_str(&stderr).unwrap();
        assert_eq!(payload["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
        assert_eq!(payload["error"]["command"], removed);
        assert_eq!(
            payload["error"]["message"],
            format!("`vz {removed}` was removed from the 0.4 public CLI")
        );
        assert!(
            payload["error"]["migration"]
                .as_str()
                .unwrap()
                .contains("vz.json")
        );
        assert!(
            payload["error"]["typed_api_migration"]
                .as_str()
                .unwrap()
                .contains("typed API")
        );
        assert_eq!(payload.as_object().unwrap().len(), 1);
        assert_eq!(payload["error"].as_object().unwrap().len(), 5);
        match uds.accept() {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
            Ok(_) => panic!("{transport}: retired command contacted daemon: {args:?}"),
        }
        match http.accept() {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock),
            Ok(_) => panic!("{transport}: retired command contacted HTTP API: {args:?}"),
        }
        assert_eq!(fs::read(&state_db).unwrap(), state_bytes);
        assert_eq!(fs::read(project.join("vz.json")).unwrap(), definition);
        assert_eq!(fs::read(project.join("sentinel")).unwrap(), sentinel);
        assert_eq!(fs::read_dir(&project).unwrap().count(), 2);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 3);
        assert!(!runtime_dir.exists());
    }
}

#[test]
fn retired_cli_is_nonmutating_under_api_http_transport() {
    assert_transport_cannot_restore_retired_commands("api-http");
}

#[test]
fn retired_cli_is_nonmutating_under_daemon_grpc_transport() {
    assert_transport_cannot_restore_retired_commands("daemon-grpc");
}
