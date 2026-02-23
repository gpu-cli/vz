//! End-to-end BuildKit integration test.
//!
//! Requirements:
//! - Apple Silicon Mac (arm64)
//! - Linux kernel artifacts installed (`~/.vz/linux/`)
//! - Network access for pulling base images
//!
//! Run with:
//! `./scripts/run-buildkit-e2e.sh`

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
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

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + network"]
async fn buildkit_builds_dockerfile_and_run_uses_built_image() {
    if !has_virtualization_entitlement() {
        eprintln!(
            "skipping buildkit_e2e: test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-buildkit-e2e.sh"
        );
        return;
    }

    init_tracing();
    let tmp = tempfile::tempdir().unwrap();
    let context_dir = tmp.path().join("context");
    std::fs::create_dir_all(&context_dir).unwrap();
    std::fs::write(
        context_dir.join("Dockerfile"),
        r#"FROM alpine:3.20
RUN echo "hello-buildkit" > /message.txt
CMD ["cat", "/message.txt"]
"#,
    )
    .unwrap();

    let config = test_config(&tmp.path().join("oci-store"));
    let tag = "buildkit-e2e:latest".to_string();
    let request = BuildRequest {
        context_dir: context_dir.clone(),
        dockerfile: "Dockerfile".into(),
        tag: tag.clone(),
        target: None,
        build_args: BTreeMap::new(),
        secrets: Vec::new(),
        no_cache: false,
        output: vz_oci_macos::buildkit::BuildOutput::VzStore,
        progress: vz_oci_macos::buildkit::BuildProgress::Plain,
    };

    let build_result = vz_oci_macos::buildkit::build_image(&config, request)
        .await
        .unwrap();
    let image_id = build_result
        .image_id
        .expect("vz store output should produce local image ID");
    assert!(!image_id.0.is_empty());

    let runtime = Runtime::new(config);
    let output = runtime.run(&tag, RunConfig::default()).await.unwrap();
    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout.trim(), "hello-buildkit");
}
