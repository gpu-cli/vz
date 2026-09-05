#![cfg(target_os = "macos")]
#![allow(clippy::expect_used)]
//! Host-only input-copy regressions, excluded from the physical driver inventory.
#[path = "support/registry_fixture_bundle.rs"]
mod registry_fixture_bundle;

use registry_fixture_bundle::copy_fixture_bundle;
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use vz_linux::{DEVELOPER_PROBE_ARCHIVE, DEVELOPER_PROBE_MARKER, KernelProfile, KernelVersion};

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn source(path: &Path, profile: KernelProfile, probe: bool) {
    fs::create_dir(path).expect("source directory");
    for (name, bytes) in [
        ("vmlinux", b"kernel".as_slice()),
        ("initramfs.img", b"initramfs"),
        ("youki", b"youki"),
    ] {
        fs::write(path.join(name), bytes).expect("artifact");
    }
    let version = KernelVersion {
        kernel: "6.12.85".into(),
        profile: Some(profile.as_str().into()),
        security_profile: Some(profile.security_profile().into()),
        busybox: "1.37.0".into(),
        agent: env!("CARGO_PKG_VERSION").into(),
        agent_protocol_revision: Some(vz_agent_proto::AGENT_PROTOCOL_REVISION),
        youki: "0.7.0".into(),
        built: None,
        sha256_vmlinux: Some(hash(b"kernel")),
        sha256_initramfs: Some(hash(b"initramfs")),
        sha256_youki: Some(hash(b"youki")),
        capabilities: Some(profile.default_capabilities()),
        developer_probe: None,
    };
    let mut json = serde_json::to_value(version).expect("version");
    if probe {
        fs::write(path.join(DEVELOPER_PROBE_ARCHIVE), b"fixture archive").expect("archive");
        json["developer_probe"] = serde_json::json!({
            "schema_version":1, "archive":DEVELOPER_PROBE_ARCHIVE, "sha256":hash(b"fixture archive"),
            "busybox_sha256":"a".repeat(64), "busybox_version":"1.37.0", "source_archive_sha256":"b".repeat(64),
            "source_inventory_sha256":"c".repeat(64), "build_provenance_sha256":"d".repeat(64),
            "marker_sha256":hash(DEVELOPER_PROBE_MARKER)
        });
    }
    fs::write(
        path.join("version.json"),
        serde_json::to_vec_pretty(&json).expect("JSON"),
    )
    .expect("version bytes");
}

#[tokio::test]
async fn exact_declared_optional_probe_and_legacy_profiles_copy_without_invented_inputs() {
    for (profile, probe) in [
        (KernelProfile::Developer, true),
        (KernelProfile::Developer, false),
        (KernelProfile::Container, false),
    ] {
        let root = tempfile::tempdir().expect("temporary root");
        let src = root.path().join("source");
        let dst = root.path().join("copy");
        source(&src, profile, probe);
        copy_fixture_bundle(&src, &dst, profile)
            .await
            .expect("verified copy");
        assert_eq!(
            fs::read_dir(&dst).expect("inventory").count(),
            if probe { 5 } else { 4 }
        );
        assert_eq!(dst.join(DEVELOPER_PROBE_ARCHIVE).exists(), probe);
        for entry in fs::read_dir(&src).expect("source files") {
            let entry = entry.expect("entry");
            assert_eq!(
                fs::read(entry.path()).expect("source bytes"),
                fs::read(dst.join(entry.file_name())).expect("copied bytes")
            );
        }
    }
}

#[tokio::test]
async fn invalid_probe_inputs_fail_before_destination_creation() {
    for kind in [
        "missing",
        "tampered",
        "redirected",
        "hardened",
        "undeclared",
        "symlink",
        "hardlink",
    ] {
        let root = tempfile::tempdir().expect("temporary root");
        let src = root.path().join("source");
        let dst = root.path().join("copy");
        let profile = if kind == "hardened" {
            KernelProfile::Container
        } else {
            KernelProfile::Developer
        };
        source(&src, profile, true);
        let archive = src.join(DEVELOPER_PROBE_ARCHIVE);
        let version = src.join("version.json");
        match kind {
            "missing" => fs::remove_file(&archive).expect("remove fixture archive"),
            "tampered" => fs::write(&archive, b"poison").expect("tamper"),
            "redirected" | "undeclared" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&fs::read(&version).expect("version")).expect("JSON");
                if kind == "redirected" {
                    value["developer_probe"]["archive"] = "../foreign".into();
                } else {
                    value
                        .as_object_mut()
                        .expect("object")
                        .remove("developer_probe");
                }
                fs::write(version, serde_json::to_vec(&value).expect("JSON"))
                    .expect("mutated version");
            }
            "symlink" => {
                let target = root.path().join("outside");
                fs::rename(&archive, &target).expect("move test archive");
                symlink(target, &archive).expect("link");
            }
            "hardlink" => {
                fs::hard_link(&archive, root.path().join("other-link")).expect("hardlink")
            }
            _ => {}
        }
        assert!(
            copy_fixture_bundle(&src, &dst, profile).await.is_err(),
            "{kind}"
        );
        assert!(!dst.exists(), "{kind}: invalid input created destination");
    }
}
