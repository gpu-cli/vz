//! End-to-end BuildKit integration test.
//!
//! Requirements:
//! - Apple Silicon Mac (arm64)
//! - Linux kernel artifacts installed (`~/.vz/linux/`)
//! - Network access for pulling base images
//!
//! Run with:
//! `./scripts/run-sandbox-vm-e2e.sh --suite buildkit`

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use vz_oci_macos::{BuildRequest, RunConfig, Runtime, RuntimeConfig};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci=debug,vz_linux=debug,vz_oci_macos=debug")
        .with_test_writer()
        .try_init();
}

fn test_config(data_dir: &std::path::Path) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        require_exact_agent_version: false,
        agent_ready_timeout: Duration::from_secs(20),
        exec_timeout: Duration::from_secs(120),
        default_memory_mb: 4096,
        ..RuntimeConfig::default()
    }
}

fn has_virtualization_entitlement() -> bool {
    let Ok(test_binary) = std::env::current_exe() else {
        return false;
    };
    let Ok(output) = Command::new("codesign")
        .arg("-d")
        .arg("--entitlements")
        .arg(":-")
        .arg(&test_binary)
        .output()
    else {
        return false;
    };

    let entitlements = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    entitlements.contains("com.apple.security.virtualization")
}

fn write_dockerfile(context_dir: &Path, body: &str) {
    std::fs::create_dir_all(context_dir).unwrap();
    std::fs::write(context_dir.join("Dockerfile"), body).unwrap();
}

fn build_request(context_dir: PathBuf, tag: String) -> BuildRequest {
    BuildRequest {
        context_dir,
        dockerfile: "Dockerfile".into(),
        tag,
        target: None,
        cache_from: Vec::new(),
        build_args: BTreeMap::new(),
        secrets: Vec::new(),
        no_cache: false,
        output: vz_oci_macos::buildkit::BuildOutput::VzStore,
        progress: vz_oci_macos::buildkit::BuildProgress::Plain,
    }
}

async fn assert_and_retain_runtime_inventory(config: &RuntimeConfig) {
    let inventory = vz_oci_macos::buildkit::buildkit_runtime_inventory(config)
        .await
        .unwrap_or_else(|error| {
            panic!("inspect retained BuildKit guest runtime inventory: {error:?}")
        });

    assert_eq!(inventory.oci_worker_binary, "/tmp/vz-buildkit-oci-runtime");
    assert_eq!(inventory.shim_target, "/usr/bin/vz-guest-agent");
    assert_eq!(inventory.runtime_binary, "/mnt/linux-bin/youki");
    assert_eq!(
        inventory.observed_runtime_paths,
        vec!["/mnt/linux-bin/youki".to_string()],
        "execution evidence must show only youki"
    );
    assert!(
        inventory
            .observed_oci_subcommands
            .iter()
            .any(|command| matches!(command.as_str(), "create" | "run")),
        "execution evidence must include a successful-build create/run invocation"
    );
    assert_eq!(
        inventory.oci_runtime_elf_paths,
        vec!["/mnt/linux-bin/youki".to_string()],
        "youki must be the only OCI runtime ELF in the BuildKit guest"
    );
    assert!(
        inventory.forbidden_runtime_paths.is_empty(),
        "forbidden runc paths remain in the BuildKit guest: {:?}",
        inventory.forbidden_runtime_paths
    );
    assert!(
        inventory
            .runtime_version
            .to_ascii_lowercase()
            .contains("youki"),
        "unexpected OCI runtime identity: {}",
        inventory.runtime_version
    );
    assert_eq!(
        inventory.buildkitd_executable,
        "/mnt/buildkit-bin/buildkitd"
    );
    assert_eq!(
        inventory.buildkitd_oci_worker_binary,
        "/tmp/vz-buildkit-oci-runtime"
    );
    assert_eq!(inventory.cgroup_filesystem, "cgroup2");

    if let Some(path) = std::env::var_os("VZ_BUILDKIT_RUNTIME_INVENTORY_EVIDENCE") {
        let mut evidence = serde_json::to_string_pretty(&inventory)
            .unwrap_or_else(|error| panic!("serialize inventory: {error:?}"));
        evidence.push('\n');
        std::fs::write(&path, evidence).unwrap_or_else(|error| {
            panic!(
                "write BuildKit runtime inventory evidence to {}: {error}",
                PathBuf::from(path).display()
            )
        });
    }
}

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + network"]
async fn buildkit_builds_dockerfile_and_run_uses_built_image() {
    if !has_virtualization_entitlement() {
        std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!("{}\n", format_args!(
            "VZ_E2E_REQUIRED_SKIP: buildkit_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite buildkit"
        )),
        ).unwrap_or_else(|error| panic!("write test diagnostic to stderr: {error}"));
        return;
    }

    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let context_dir = tmp.path().join("context");
    write_dockerfile(
        &context_dir,
        r#"FROM alpine:3.20
RUN echo "hello-buildkit" > /message.txt
CMD ["cat", "/message.txt"]
"#,
    );

    let config = test_config(&tmp.path().join("oci-store"));
    let tag = "buildkit-e2e:latest".to_string();
    let request = build_request(context_dir.clone(), tag.clone());

    let build_result = vz_oci_macos::buildkit::build_image(&config, request)
        .await
        .unwrap();
    let image_id = build_result
        .image_id
        .unwrap_or_else(|| panic!("vz store output should produce local image ID"));
    assert!(!image_id.0.is_empty());

    assert_and_retain_runtime_inventory(&config).await;
    vz_oci_macos::buildkit::shutdown_buildkit_vm()
        .await
        .unwrap();

    let runtime = Runtime::new(config);
    let output = runtime.run(&tag, RunConfig::default()).await.unwrap();
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), "hello-buildkit");
}

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + network"]
async fn buildkit_cache_disk_usage_health_smoke() {
    if !has_virtualization_entitlement() {
        std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!("{}\n", format_args!(
            "VZ_E2E_REQUIRED_SKIP: buildkit_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite buildkit"
        )),
        ).unwrap_or_else(|error| panic!("write test diagnostic to stderr: {error}"));
        return;
    }

    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(&tmp.path().join("oci-store"));
    let usage = vz_oci_macos::buildkit::cache_disk_usage(&config)
        .await
        .unwrap();
    assert!(
        !usage.trim().is_empty(),
        "expected non-empty buildctl du output"
    );
    vz_oci_macos::buildkit::shutdown_buildkit_vm()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + network"]
async fn buildkit_cache_survives_context_switch_vm_restart() {
    if !has_virtualization_entitlement() {
        std::io::Write::write_fmt(
            &mut std::io::stderr().lock(),
            format_args!("{}\n", format_args!(
            "VZ_E2E_REQUIRED_SKIP: buildkit_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite buildkit"
        )),
        ).unwrap_or_else(|error| panic!("write test diagnostic to stderr: {error}"));
        return;
    }

    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let config = test_config(&tmp.path().join("oci-store"));
    let context_a = tmp.path().join("context-a");
    let context_b = tmp.path().join("context-b");

    let dockerfile = r#"FROM alpine:3.20
RUN echo "cache-probe" > /cache-probe.txt
"#;
    write_dockerfile(&context_a, dockerfile);
    write_dockerfile(&context_b, dockerfile);

    let first_a = build_request(context_a.clone(), "buildkit-cache-a:first".to_string());
    vz_oci_macos::buildkit::build_image(&config, first_a)
        .await
        .unwrap();

    // This build uses a different context path, forcing a VM recycle in the
    // current shared-mount implementation.
    let first_b = build_request(context_b.clone(), "buildkit-cache-b:first".to_string());
    vz_oci_macos::buildkit::build_image(&config, first_b)
        .await
        .unwrap();

    let second_a = build_request(context_a, "buildkit-cache-a:second".to_string());
    let mut output = Vec::new();
    vz_oci_macos::buildkit::build_image_with_events(&config, second_a, |event| {
        if let vz_oci_macos::buildkit::BuildEvent::Output { chunk, .. } = event {
            output.extend_from_slice(&chunk);
        }
    })
    .await
    .unwrap();

    let output_text = String::from_utf8_lossy(&output).to_ascii_lowercase();
    assert!(
        output_text.contains("cached"),
        "expected BuildKit cache hit after VM restart, output was:\n{}",
        output_text
    );
    vz_oci_macos::buildkit::shutdown_buildkit_vm()
        .await
        .unwrap();
}
