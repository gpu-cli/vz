#![cfg(target_os = "macos")]
#![allow(clippy::expect_used)]

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

const INSTALLED_DAEMON_ENV: &str = "VZ_TEST_INSTALLED_DAEMON";
const INSTALLED_DAEMON_SHA256_ENV: &str = "VZ_TEST_INSTALLED_DAEMON_SHA256";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathSnapshot {
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    links: u64,
    length: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    bytes: Option<Vec<u8>>,
    symlink_target: Option<PathBuf>,
}

impl PathSnapshot {
    fn capture(path: &Path) -> Self {
        let metadata = fs::symlink_metadata(path)
            .unwrap_or_else(|error| panic!("inspect preserved path {}: {error}", path.display()));
        let (bytes, symlink_target) = if metadata.file_type().is_symlink() {
            (
                None,
                Some(fs::read_link(path).unwrap_or_else(|error| {
                    panic!("read preserved symlink {}: {error}", path.display())
                })),
            )
        } else {
            (
                Some(fs::read(path).unwrap_or_else(|error| {
                    panic!("read preserved file {}: {error}", path.display())
                })),
                None,
            )
        };
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            links: metadata.nlink(),
            length: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
            bytes,
            symlink_target,
        }
    }
}

fn sha256_file(path: &Path) -> String {
    let mut file = File::open(path)
        .unwrap_or_else(|error| panic!("open installed daemon {}: {error}", path.display()));
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .unwrap_or_else(|error| panic!("hash installed daemon {}: {error}", path.display()));
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    format!("{:x}", hasher.finalize())
}

fn installed_daemon() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os(INSTALLED_DAEMON_ENV)
            .unwrap_or_else(|| panic!("{INSTALLED_DAEMON_ENV} is required")),
    );
    let expected_digest = std::env::var(INSTALLED_DAEMON_SHA256_ENV)
        .unwrap_or_else(|_| panic!("{INSTALLED_DAEMON_SHA256_ENV} is required"));
    assert!(
        path.is_absolute(),
        "{INSTALLED_DAEMON_ENV} must name an absolute path, found {}",
        path.display()
    );
    let metadata = fs::symlink_metadata(&path).unwrap_or_else(|error| {
        panic!(
            "{INSTALLED_DAEMON_ENV} must name an existing binary {}: {error}",
            path.display()
        )
    });
    assert!(
        metadata.file_type().is_file(),
        "{INSTALLED_DAEMON_ENV} must name a regular non-symlink file"
    );
    assert!(
        metadata.permissions().mode() & 0o111 != 0,
        "{INSTALLED_DAEMON_ENV} must name an executable file"
    );
    assert!(
        expected_digest.len() == 64
            && expected_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{INSTALLED_DAEMON_SHA256_ENV} must be a canonical lowercase SHA-256"
    );
    let actual_digest = sha256_file(&path);
    assert_eq!(
        actual_digest, expected_digest,
        "installed daemon digest differs from exact supplied evidence"
    );
    let mut test_stderr = std::io::stderr().lock();
    writeln!(
        test_stderr,
        "installed_daemon={}\ninstalled_daemon_sha256={expected_digest}",
        path.display()
    )
    .expect("write installed daemon identity evidence");
    path
}

fn wait_bounded(mut command: Command, case: &str) -> Output {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("spawn installed daemon for {case}: {error}"));
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        if child
            .try_wait()
            .unwrap_or_else(|error| panic!("poll installed daemon for {case}: {error}"))
            .is_some()
        {
            return child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect installed daemon for {case}: {error}"));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect timed-out daemon for {case}: {error}"));
            panic!(
                "installed daemon did not reject {case} within {PROCESS_TIMEOUT:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn directory_names(path: &Path) -> Vec<String> {
    let mut names = fs::read_dir(path)
        .unwrap_or_else(|error| panic!("read case directory {}: {error}", path.display()))
        .map(|entry| {
            entry
                .expect("read case directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn assert_rejected_without_effects(
    binary: &Path,
    case_root: &Path,
    catalog_argument: &OsStr,
    preserved_paths: &[&Path],
    case: &str,
) {
    let before_paths = preserved_paths
        .iter()
        .map(|path| ((*path).to_path_buf(), PathSnapshot::capture(path)))
        .collect::<Vec<_>>();
    let before_names = directory_names(case_root);
    let effects = case_root.join("effects");
    assert!(!effects.exists(), "{case} effects root starts absent");

    let mut command = Command::new(binary);
    command
        .current_dir(case_root)
        .arg("--machine-target-catalog")
        .arg(catalog_argument)
        .arg("--state-store-path")
        .arg(effects.join("state/stack-state.db"))
        .arg("--runtime-data-dir")
        .arg(effects.join("runtime"))
        .arg("--socket-path")
        .arg(effects.join("socket/runtimed.sock"))
        .env_remove("RUST_LOG")
        .env_remove("RUST_BACKTRACE");
    let output = wait_bounded(command, case);

    assert!(
        !output.status.success(),
        "installed daemon unexpectedly accepted {case}"
    );
    assert!(
        output.stdout.len() <= MAX_DIAGNOSTIC_BYTES && output.stderr.len() <= MAX_DIAGNOSTIC_BYTES,
        "{case} diagnostics exceeded the bounded startup response"
    );
    {
        let mut test_stderr = std::io::stderr().lock();
        writeln!(
            test_stderr,
            "--- {case}: status={} stderr_bytes={} ---",
            output.status,
            output.stderr.len()
        )
        .expect("write installed daemon evidence header");
        test_stderr
            .write_all(&output.stderr)
            .expect("write raw installed daemon stderr evidence");
        if !output.stderr.ends_with(b"\n") {
            writeln!(test_stderr).expect("terminate installed daemon stderr evidence");
        }
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to load explicit Machine target catalog"),
        "{case} returned an unexpected error: {stderr}"
    );
    assert!(
        !effects.exists(),
        "{case} created daemon state, runtime, socket, log, PID, or lock paths"
    );
    assert_eq!(
        directory_names(case_root),
        before_names,
        "{case} changed the isolated case inventory"
    );
    for (path, before) in before_paths {
        assert_eq!(
            PathSnapshot::capture(&path),
            before,
            "{case} changed catalog path identity or contents at {}",
            path.display()
        );
    }
}

fn write_catalog(path: &Path, contents: &[u8], mode: u32) {
    fs::write(path, contents)
        .unwrap_or_else(|error| panic!("write catalog fixture {}: {error}", path.display()));
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .unwrap_or_else(|error| panic!("set catalog fixture mode {}: {error}", path.display()));
}

#[test]
#[ignore = "requires an exact installed release-built vz-runtimed binary and digest"]
fn installed_daemon_rejects_invalid_machine_target_catalogs_without_effects() {
    let binary = installed_daemon();
    let root = tempfile::tempdir().expect("isolated catalog CLI root");

    let invalid_schema_root = root.path().join("invalid-schema");
    fs::create_dir(&invalid_schema_root).expect("create invalid-schema case");
    let invalid_schema = invalid_schema_root.join("catalog.json");
    write_catalog(
        &invalid_schema,
        br#"{"schema_version":0,"linux":[]}
"#,
        0o600,
    );
    assert_rejected_without_effects(
        &binary,
        &invalid_schema_root,
        invalid_schema.as_os_str(),
        &[&invalid_schema],
        "invalid catalog schema",
    );

    let relative_root = root.path().join("relative-path");
    fs::create_dir(&relative_root).expect("create relative-path case");
    let relative_catalog = relative_root.join("catalog.json");
    write_catalog(
        &relative_catalog,
        br#"{"schema_version":1,"linux":[]}
"#,
        0o600,
    );
    assert_rejected_without_effects(
        &binary,
        &relative_root,
        OsStr::new("catalog.json"),
        &[&relative_catalog],
        "relative catalog path",
    );

    let symlink_root = root.path().join("symlink-path");
    fs::create_dir(&symlink_root).expect("create symlink-path case");
    let symlink_target = symlink_root.join("target.json");
    let symlink_catalog = symlink_root.join("catalog.json");
    write_catalog(
        &symlink_target,
        br#"{"schema_version":1,"linux":[]}
"#,
        0o600,
    );
    symlink(&symlink_target, &symlink_catalog).expect("create catalog symlink");
    assert_rejected_without_effects(
        &binary,
        &symlink_root,
        symlink_catalog.as_os_str(),
        &[&symlink_target, &symlink_catalog],
        "symlink catalog path",
    );

    let untrusted_root = root.path().join("untrusted-file");
    fs::create_dir(&untrusted_root).expect("create untrusted-file case");
    let untrusted_catalog = untrusted_root.join("catalog.json");
    write_catalog(
        &untrusted_catalog,
        br#"{"schema_version":1,"linux":[]}
"#,
        0o666,
    );
    assert_rejected_without_effects(
        &binary,
        &untrusted_root,
        untrusted_catalog.as_os_str(),
        &[&untrusted_catalog],
        "group/world-writable catalog file",
    );
}
