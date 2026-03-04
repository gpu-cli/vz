#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::apply::apply_with_state_path;
use super::*;
use crate::commands::vm_base::BASE_CHANNEL_STABLE;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use tempfile::{TempDir, tempdir};

const ACTIVE_BASE_ID: &str = "macos-15.3.1-24D70-arm64-64g";
const PREVIOUS_BASE_ID: &str = "macos-14.6-23G80-arm64-64g";
const RETIRED_BASE_ID: &str = "macos-13.6.7-22H123-arm64-64g";

fn make_signing_key_pair() -> Ed25519KeyPair {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate test key");
    Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse test key")
}

fn valid_manifest(key_pair: &Ed25519KeyPair, payload: &[u8]) -> PatchBundleManifest {
    let operations = vec![
        PatchOperation::Mkdir {
            path: "/usr/local/libexec".to_string(),
            mode: Some(0o755),
        },
        PatchOperation::WriteFile {
            path: "/usr/local/libexec/vz-agent".to_string(),
            content_digest: sha256_bytes_hex(b"agent-binary"),
            mode: Some(0o755),
        },
        PatchOperation::SetOwner {
            path: "/usr/local/libexec/vz-agent".to_string(),
            uid: 0,
            gid: 0,
        },
        PatchOperation::SetMode {
            path: "/usr/local/libexec/vz-agent".to_string(),
            mode: 0o755,
        },
        PatchOperation::Symlink {
            path: "/usr/local/bin/vz-agent".to_string(),
            target: "/usr/local/libexec/vz-agent".to_string(),
        },
        PatchOperation::DeleteFile {
            path: "/tmp/old-vz-agent".to_string(),
        },
    ];

    PatchBundleManifest {
        bundle_id: "vz-cih-2-1-bundle".to_string(),
        patch_version: "1.0.0".to_string(),
        target_base_id: ACTIVE_BASE_ID.to_string(),
        target_base_fingerprint: BundleBaseFingerprint {
            img_sha256: "1".repeat(64),
            aux_sha256: "2".repeat(64),
            hwmodel_sha256: "3".repeat(64),
            machineid_sha256: "4".repeat(64),
        },
        operations_digest: operations_digest_hex(&operations).expect("hash operations"),
        payload_digest: sha256_bytes_hex(payload),
        post_state_hashes: BTreeMap::from([(
            "/usr/local/bin/vz-agent".to_string(),
            sha256_bytes_hex(b"post-state-vz-agent"),
        )]),
        created_at: "2026-02-24T17:20:00Z".to_string(),
        signing_identity: format!(
            "ed25519:{}",
            base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
        ),
        operations,
    }
}

fn write_signed_bundle(
    dir: &Path,
    key_pair: &Ed25519KeyPair,
    manifest: &PatchBundleManifest,
    payload: &[u8],
) {
    let manifest_bytes = serde_json::to_vec_pretty(manifest).expect("serialize manifest");
    fs::write(dir.join(MANIFEST_FILE), &manifest_bytes).expect("write manifest");
    fs::write(dir.join(PAYLOAD_FILE), payload).expect("write payload");
    let signature = key_pair.sign(&manifest_bytes);
    fs::write(dir.join(SIGNATURE_FILE), signature.as_ref()).expect("write signature");
}

fn create_valid_bundle() -> TempDir {
    let dir = tempdir().expect("create temp dir");
    let key_pair = make_signing_key_pair();
    let payload = b"payload archive bytes";
    let manifest = valid_manifest(&key_pair, payload);
    write_signed_bundle(dir.path(), &key_pair, &manifest, payload);
    dir
}

fn build_payload_archive(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut payload = Vec::new();
    let encoder = zstd::Encoder::new(&mut payload, 0).expect("create zstd encoder");
    let mut builder = tar::Builder::new(encoder);

    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (digest, bytes) in sorted {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, digest, bytes.as_slice())
            .expect("append payload entry");
    }

    let encoder = builder.into_inner().expect("finish tar builder");
    encoder.finish().expect("finish zstd encoding");
    payload
}

fn write_test_signing_key(path: &Path) {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate test key");
    fs::write(path, pkcs8.as_ref()).expect("write signing key");
}

fn default_test_base_fingerprint() -> BundleBaseFingerprint {
    BundleBaseFingerprint {
        img_sha256: "1".repeat(64),
        aux_sha256: "2".repeat(64),
        hwmodel_sha256: "3".repeat(64),
        machineid_sha256: "4".repeat(64),
    }
}

fn build_apply_bundle_with_target(
    root: &Path,
    bundle_id: &str,
    target_base_id: &str,
    target_base_fingerprint: BundleBaseFingerprint,
    operations: Vec<PatchOperation>,
    post_state_hashes: BTreeMap<String, String>,
    payload_entries: &[(String, Vec<u8>)],
) -> TempDir {
    let bundle = tempdir().expect("create bundle");
    let key_pair = make_signing_key_pair();
    let payload = build_payload_archive(payload_entries);

    let manifest = PatchBundleManifest {
        bundle_id: bundle_id.to_string(),
        patch_version: "1.0.1".to_string(),
        target_base_id: target_base_id.to_string(),
        target_base_fingerprint,
        operations_digest: operations_digest_hex(&operations).expect("hash operations"),
        payload_digest: sha256_bytes_hex(&payload),
        post_state_hashes,
        created_at: "2026-02-24T18:40:00Z".to_string(),
        signing_identity: format!(
            "ed25519:{}",
            base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
        ),
        operations,
    };

    write_signed_bundle(bundle.path(), &key_pair, &manifest, &payload);
    assert!(root.exists());
    bundle
}

fn build_apply_bundle(
    root: &Path,
    operations: Vec<PatchOperation>,
    post_state_hashes: BTreeMap<String, String>,
    payload_entries: &[(String, Vec<u8>)],
) -> TempDir {
    build_apply_bundle_with_target(
        root,
        "vz-cih-2-2-apply",
        ACTIVE_BASE_ID,
        default_test_base_fingerprint(),
        operations,
        post_state_hashes,
        payload_entries,
    )
}

fn apply_with_test_state(
    bundle: &Path,
    root: &Path,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    apply_with_state_path(
        ApplyArgs {
            bundle: bundle.to_path_buf(),
            root: Some(root.to_path_buf()),
            image: None,
        },
        patch_state_path,
    )
}

fn write_test_image_sidecars(image_path: &Path) {
    fs::write(image_path.with_extension("aux"), b"aux-sidecar").expect("write aux sidecar");
    fs::write(image_path.with_extension("hwmodel"), b"hwmodel-sidecar")
        .expect("write hwmodel sidecar");
    fs::write(image_path.with_extension("machineid"), b"machineid-sidecar")
        .expect("write machineid sidecar");
}

#[test]
fn verify_bundle_valid_path() {
    let bundle = create_valid_bundle();
    let manifest = verify_bundle(bundle.path()).expect("bundle should verify");
    assert_eq!(manifest.bundle_id, "vz-cih-2-1-bundle");
    assert_eq!(manifest.patch_version, "1.0.0");
}

#[test]
fn patch_create_builds_signed_bundle_from_inputs() {
    let dir = tempdir().expect("create temp dir");
    let bundle_dir = dir.path().join("created-bundle.vzpatch");
    let payload_dir = dir.path().join("payload");
    fs::create_dir_all(&payload_dir).expect("create payload dir");

    let payload_bytes = b"tool-bytes".to_vec();
    let payload_digest = sha256_bytes_hex(&payload_bytes);
    fs::write(payload_dir.join(&payload_digest), &payload_bytes).expect("write payload entry");

    let operations = vec![
        PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: payload_digest.clone(),
            mode: Some(0o755),
        },
        PatchOperation::Symlink {
            path: "/usr/local/bin/tool".to_string(),
            target: "/opt/tool".to_string(),
        },
    ];
    let operations_path = dir.path().join("operations.json");
    fs::write(
        &operations_path,
        serde_json::to_vec_pretty(&operations).expect("serialize operations"),
    )
    .expect("write operations file");

    let signing_key_path = dir.path().join("signing-key.pkcs8");
    write_test_signing_key(&signing_key_path);

    create(CreateArgs {
        bundle: bundle_dir.clone(),
        base_id: BASE_CHANNEL_STABLE.to_string(),
        operations: Some(operations_path),
        payload_dir: Some(payload_dir),
        signing_key: signing_key_path,
        post_state_hashes: None,
        patch_version: "2.0.0".to_string(),
        bundle_id: Some("bundle-create-test".to_string()),
        created_at: Some("2026-02-24T19:00:00Z".to_string()),
        write_file: Vec::new(),
        mkdir: Vec::new(),
        symlink: Vec::new(),
        delete_file: Vec::new(),
        set_mode: Vec::new(),
        set_owner: Vec::new(),
    })
    .expect("create should succeed");

    assert!(bundle_dir.join(MANIFEST_FILE).exists());
    assert!(bundle_dir.join(PAYLOAD_FILE).exists());
    assert!(bundle_dir.join(SIGNATURE_FILE).exists());

    let manifest = verify_bundle(&bundle_dir).expect("created bundle should verify");
    assert_eq!(manifest.bundle_id, "bundle-create-test");
    assert_eq!(manifest.patch_version, "2.0.0");
    assert_eq!(manifest.target_base_id, ACTIVE_BASE_ID);
    assert_eq!(manifest.operations, operations);
    assert_eq!(
        manifest
            .post_state_hashes
            .get("/opt/tool")
            .expect("write file hash"),
        &payload_digest
    );
    assert_eq!(
        manifest
            .post_state_hashes
            .get("/usr/local/bin/tool")
            .expect("symlink hash"),
        &sha256_bytes_hex(Path::new("/opt/tool").as_os_str().as_bytes())
    );
}

#[test]
fn patch_create_rejects_payload_digest_mismatch() {
    let dir = tempdir().expect("create temp dir");
    let bundle_dir = dir.path().join("created-bundle.vzpatch");
    let payload_dir = dir.path().join("payload");
    fs::create_dir_all(&payload_dir).expect("create payload dir");

    let expected_digest = sha256_bytes_hex(b"expected");
    fs::write(payload_dir.join(&expected_digest), b"unexpected").expect("write payload entry");

    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: expected_digest,
        mode: Some(0o755),
    }];
    let operations_path = dir.path().join("operations.json");
    fs::write(
        &operations_path,
        serde_json::to_vec_pretty(&operations).expect("serialize operations"),
    )
    .expect("write operations file");

    let signing_key_path = dir.path().join("signing-key.pkcs8");
    write_test_signing_key(&signing_key_path);

    let err = create(CreateArgs {
        bundle: bundle_dir,
        base_id: BASE_CHANNEL_STABLE.to_string(),
        operations: Some(operations_path),
        payload_dir: Some(payload_dir),
        signing_key: signing_key_path,
        post_state_hashes: None,
        patch_version: "2.0.0".to_string(),
        bundle_id: None,
        created_at: None,
        write_file: Vec::new(),
        mkdir: Vec::new(),
        symlink: Vec::new(),
        delete_file: Vec::new(),
        set_mode: Vec::new(),
        set_owner: Vec::new(),
    })
    .expect_err("mismatched payload digest should fail");
    assert!(format!("{err:#}").contains("digest mismatch"));
}

#[test]
fn patch_create_inline_mode_builds_bundle_from_write_specs() {
    let dir = tempdir().expect("create temp dir");
    let bundle_dir = dir.path().join("created-inline-bundle.vzpatch");
    let host_file = dir.path().join("vz-agent");
    fs::write(&host_file, b"inline-agent-bytes").expect("write host file");

    create(CreateArgs {
        bundle: bundle_dir.clone(),
        base_id: BASE_CHANNEL_STABLE.to_string(),
        operations: None,
        payload_dir: None,
        signing_key: {
            let path = dir.path().join("signing-key.pkcs8");
            write_test_signing_key(&path);
            path
        },
        post_state_hashes: None,
        patch_version: "2.1.0".to_string(),
        bundle_id: Some("bundle-inline-test".to_string()),
        created_at: Some("2026-02-24T19:30:00Z".to_string()),
        write_file: vec![format!("{}:/opt/vz-agent:755", host_file.display())],
        mkdir: vec!["/opt:755".to_string()],
        symlink: vec!["/usr/local/bin/vz-agent:/opt/vz-agent".to_string()],
        delete_file: Vec::new(),
        set_mode: vec!["/opt/vz-agent:755".to_string()],
        set_owner: Vec::new(),
    })
    .expect("inline create should succeed");

    let manifest = verify_bundle(&bundle_dir).expect("created bundle should verify");
    assert_eq!(manifest.bundle_id, "bundle-inline-test");
    assert_eq!(manifest.patch_version, "2.1.0");
    assert_eq!(manifest.target_base_id, ACTIVE_BASE_ID);
    assert!(manifest.operations.iter().any(|operation| matches!(
        operation,
        PatchOperation::WriteFile { path, .. } if path == "/opt/vz-agent"
    )));
    assert!(manifest.operations.iter().any(|operation| matches!(
        operation,
        PatchOperation::Symlink { path, target }
            if path == "/usr/local/bin/vz-agent" && target == "/opt/vz-agent"
    )));
    assert_eq!(
        manifest
            .post_state_hashes
            .get("/usr/local/bin/vz-agent")
            .expect("symlink hash"),
        &sha256_bytes_hex(Path::new("/opt/vz-agent").as_os_str().as_bytes())
    );
}

#[test]
fn patch_create_rejects_mixed_input_modes() {
    let dir = tempdir().expect("create temp dir");
    let bundle_dir = dir.path().join("mixed-mode-bundle.vzpatch");
    let payload_dir = dir.path().join("payload");
    fs::create_dir_all(&payload_dir).expect("create payload dir");

    let payload_bytes = b"tool-bytes".to_vec();
    let payload_digest = sha256_bytes_hex(&payload_bytes);
    fs::write(payload_dir.join(&payload_digest), &payload_bytes).expect("write payload entry");

    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: payload_digest,
        mode: Some(0o755),
    }];
    let operations_path = dir.path().join("operations.json");
    fs::write(
        &operations_path,
        serde_json::to_vec_pretty(&operations).expect("serialize operations"),
    )
    .expect("write operations file");

    let signing_key_path = dir.path().join("signing-key.pkcs8");
    write_test_signing_key(&signing_key_path);

    let err = create(CreateArgs {
        bundle: bundle_dir,
        base_id: BASE_CHANNEL_STABLE.to_string(),
        operations: Some(operations_path),
        payload_dir: Some(payload_dir),
        signing_key: signing_key_path,
        post_state_hashes: None,
        patch_version: "2.0.0".to_string(),
        bundle_id: None,
        created_at: None,
        write_file: vec![format!(
            "{}:/opt/tool:755",
            dir.path().join("some-file").display()
        )],
        mkdir: Vec::new(),
        symlink: Vec::new(),
        delete_file: Vec::new(),
        set_mode: Vec::new(),
        set_owner: Vec::new(),
    })
    .expect_err("mixing input modes should fail");
    assert!(format!("{err:#}").contains("choose one create input mode"));
}

#[test]
fn verify_bundle_signature_mismatch_fails() {
    let bundle = create_valid_bundle();
    fs::write(
        bundle.path().join(SIGNATURE_FILE),
        [0u8; ED25519_SIGNATURE_LEN],
    )
    .expect("overwrite signature");

    let err = verify_bundle(bundle.path()).expect_err("signature mismatch should fail");
    assert!(err.to_string().contains("signature verification failed"));
}

#[test]
fn verify_bundle_payload_digest_mismatch_fails() {
    let bundle = create_valid_bundle();
    fs::write(bundle.path().join(PAYLOAD_FILE), b"tampered payload").expect("overwrite payload");

    let err = verify_bundle(bundle.path()).expect_err("payload digest mismatch should fail");
    assert!(err.to_string().contains("payload digest mismatch"));
}

#[test]
fn verify_bundle_operations_digest_mismatch_fails() {
    let dir = tempdir().expect("create temp dir");
    let key_pair = make_signing_key_pair();
    let payload = b"payload archive bytes";
    let mut manifest = valid_manifest(&key_pair, payload);
    manifest.operations_digest = "0".repeat(64);
    write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

    let err = verify_bundle(dir.path()).expect_err("operations digest mismatch should fail");
    assert!(err.to_string().contains("operations digest mismatch"));
}

#[test]
fn verify_bundle_malformed_manifest_metadata_fails() {
    let dir = tempdir().expect("create temp dir");
    let key_pair = make_signing_key_pair();
    let payload = b"payload archive bytes";
    let mut manifest = valid_manifest(&key_pair, payload);
    manifest.bundle_id = " ".to_string();
    write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

    let err = verify_bundle(dir.path()).expect_err("malformed metadata should fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("manifest.bundle_id"));
}

#[test]
fn patch_verify_rejects_unsupported_target_base_descriptor() {
    let dir = tempdir().expect("create temp dir");
    let key_pair = make_signing_key_pair();
    let payload = b"payload archive bytes";
    let mut manifest = valid_manifest(&key_pair, payload);
    manifest.target_base_id = "macos-99.9.9-unknown-arm64-64g".to_string();
    write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

    let err = verify(VerifyArgs {
        bundle: dir.path().to_path_buf(),
    })
    .expect_err("unsupported target base should fail verify");
    let msg = format!("{err:#}");
    assert!(msg.contains("unsupported or retired base"));
    assert!(msg.contains("unknown base selector"));
    assert!(msg.contains("vz vm mac init --base stable"));
}

#[test]
fn patch_apply_rejects_retired_target_base_descriptor() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("opt")).expect("create parent");

    let bytes = b"patched-bytes".to_vec();
    let digest = sha256_bytes_hex(&bytes);
    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: digest.clone(),
        mode: Some(0o755),
    }];
    let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
    let bundle = build_apply_bundle_with_target(
        root.path(),
        "vz-cih-2-2-retired",
        RETIRED_BASE_ID,
        default_test_base_fingerprint(),
        operations,
        post_state_hashes,
        &[(digest, bytes)],
    );

    let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect_err("retired target base should fail apply");
    let msg = format!("{err:#}");
    assert!(msg.contains("unsupported or retired base"));
    assert!(msg.contains("retired base"));
    assert!(msg.contains(RETIRED_BASE_ID));
    assert!(msg.contains("vz vm mac init --base stable"));
    assert!(msg.contains(BASE_CHANNEL_STABLE));
    assert!(!root.path().join("opt/tool").exists());
}

#[test]
fn patch_state_roundtrip_load_save() {
    let dir = tempdir().expect("create temp dir");
    let state_path = dir.path().join("patch-state.json");

    let mut state = PatchApplyState::default();
    let receipt = PatchApplyReceipt {
        apply_target: "/tmp/target".to_string(),
        apply_target_digest: "a".repeat(64),
        bundle_id: "bundle-a".to_string(),
        target_base_id: "base-a".to_string(),
        target_base_fingerprint: default_test_base_fingerprint(),
        operations_digest: "b".repeat(64),
        payload_digest: "c".repeat(64),
    };
    state.record_receipt(receipt.clone());
    state.save(&state_path).expect("save patch state");

    let loaded = PatchApplyState::load(&state_path).expect("load patch state");
    assert_eq!(loaded, state);
    assert!(loaded.has_receipt(&receipt));
}

#[test]
fn patch_state_malformed_file_is_actionable() {
    let dir = tempdir().expect("create temp dir");
    let state_path = dir.path().join("patch-state.json");
    fs::write(&state_path, "{ not-valid-json").expect("write malformed state");

    let err = PatchApplyState::load(&state_path).expect_err("malformed state should return error");
    let message = format!("{err:#}");
    assert!(message.contains("patch state file"));
    assert!(message.contains("is malformed"));
    assert!(message.contains("Move or delete"));
}

#[test]
fn apply_first_apply_writes_receipt() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("opt")).expect("create parent");

    let bytes = b"patched-bytes".to_vec();
    let digest = sha256_bytes_hex(&bytes);
    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: digest.clone(),
        mode: Some(0o755),
    }];
    let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
    let bundle = build_apply_bundle(
        root.path(),
        operations,
        post_state_hashes,
        &[(digest.clone(), bytes)],
    );

    apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect("first apply should succeed");

    let manifest = verify_bundle(bundle.path()).expect("manifest should verify");
    let canonical_root = fs::canonicalize(root.path()).expect("canonicalize root");
    let expected_receipt =
        PatchApplyReceipt::from_manifest(&canonical_root, &manifest).expect("build receipt");

    let state = PatchApplyState::load(&patch_state_path).expect("load state");
    assert!(patch_state_path.exists());
    assert_eq!(state.receipts.len(), 1);
    assert!(state.has_receipt(&expected_receipt));
}

#[test]
fn apply_second_identical_apply_noops() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("opt")).expect("create parent");

    let bytes = b"patched-bytes".to_vec();
    let digest = sha256_bytes_hex(&bytes);
    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: digest.clone(),
        mode: Some(0o755),
    }];
    let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
    let bundle = build_apply_bundle(
        root.path(),
        operations,
        post_state_hashes,
        &[(digest.clone(), bytes)],
    );

    apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect("first apply should succeed");
    fs::write(root.path().join("opt/tool"), b"drifted").expect("mutate post first apply");
    apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect("second apply should no-op");

    assert_eq!(
        fs::read(root.path().join("opt/tool")).expect("read tool after no-op"),
        b"drifted"
    );
    let state = PatchApplyState::load(&patch_state_path).expect("load state");
    assert_eq!(state.receipts.len(), 1);
}

#[test]
fn apply_receipt_base_mismatch_fails_with_diagnostics() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("opt")).expect("create parent");

    let bytes = b"patched-bytes".to_vec();
    let digest = sha256_bytes_hex(&bytes);
    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: digest.clone(),
        mode: Some(0o755),
    }];
    let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
    let first_bundle = build_apply_bundle_with_target(
        root.path(),
        "vz-cih-2-2-apply",
        ACTIVE_BASE_ID,
        default_test_base_fingerprint(),
        operations.clone(),
        post_state_hashes.clone(),
        &[(digest.clone(), bytes.clone())],
    );
    apply_with_test_state(first_bundle.path(), root.path(), &patch_state_path)
        .expect("first apply should succeed");

    let second_bundle = build_apply_bundle_with_target(
        root.path(),
        "vz-cih-2-2-apply",
        PREVIOUS_BASE_ID,
        BundleBaseFingerprint {
            img_sha256: "a".repeat(64),
            aux_sha256: "2".repeat(64),
            hwmodel_sha256: "3".repeat(64),
            machineid_sha256: "4".repeat(64),
        },
        operations,
        post_state_hashes,
        &[(digest, bytes)],
    );
    let err = apply_with_test_state(second_bundle.path(), root.path(), &patch_state_path)
        .expect_err("base mismatch should fail");
    let message = format!("{err:#}");
    assert!(message.contains("patch receipt mismatch"));
    assert!(message.contains("expected(existing receipt):"));
    assert!(message.contains("actual(requested apply):"));
    assert!(message.contains(ACTIVE_BASE_ID));
    assert!(message.contains(PREVIOUS_BASE_ID));
    assert!(message.contains("img_sha256=aaaaaaaa"));
    assert_eq!(
        fs::read(root.path().join("opt/tool")).expect("file should remain from first apply"),
        b"patched-bytes"
    );
}

#[test]
fn apply_successful_deterministic_replay() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("usr/local/bin")).expect("create symlink parent");
    fs::create_dir_all(root.path().join("tmp")).expect("create tmp");
    fs::write(root.path().join("tmp/old-vz-agent"), b"legacy").expect("write old file");

    let owner = fs::metadata(root.path()).expect("root metadata");
    let uid = owner.uid();
    let gid = owner.gid();
    let agent_bytes = b"agent-binary-v2".to_vec();
    let agent_digest = sha256_bytes_hex(&agent_bytes);
    let link_target = "/usr/local/libexec/vz-agent";

    let operations = vec![
        PatchOperation::Mkdir {
            path: "/usr/local/libexec".to_string(),
            mode: Some(0o755),
        },
        PatchOperation::WriteFile {
            path: "/usr/local/libexec/vz-agent".to_string(),
            content_digest: agent_digest.clone(),
            mode: Some(0o700),
        },
        PatchOperation::Symlink {
            path: "/usr/local/bin/vz-agent".to_string(),
            target: link_target.to_string(),
        },
        PatchOperation::SetOwner {
            path: "/usr/local/libexec/vz-agent".to_string(),
            uid,
            gid,
        },
        PatchOperation::SetMode {
            path: "/usr/local/libexec/vz-agent".to_string(),
            mode: 0o755,
        },
        PatchOperation::DeleteFile {
            path: "/tmp/old-vz-agent".to_string(),
        },
    ];
    let post_state_hashes = BTreeMap::from([
        (
            "/usr/local/libexec/vz-agent".to_string(),
            agent_digest.clone(),
        ),
        (
            "/usr/local/bin/vz-agent".to_string(),
            sha256_bytes_hex(Path::new(link_target).as_os_str().as_bytes()),
        ),
    ]);

    let bundle = build_apply_bundle(
        root.path(),
        operations,
        post_state_hashes,
        &[(agent_digest.clone(), agent_bytes.clone())],
    );

    apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect("first apply should succeed");
    apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect("second apply should be deterministic");

    let file_path = root.path().join("usr/local/libexec/vz-agent");
    assert_eq!(fs::read(&file_path).expect("read file"), agent_bytes);
    assert_eq!(
        fs::metadata(&file_path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o7777,
        0o755
    );
    assert_eq!(
        fs::read_link(root.path().join("usr/local/bin/vz-agent")).expect("read symlink"),
        PathBuf::from(link_target)
    );
    assert!(!root.path().join("tmp/old-vz-agent").exists());
}

#[test]
fn apply_rejects_path_traversal_before_mutation() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("safe")).expect("create safe directory");

    let first_bytes = b"first".to_vec();
    let second_bytes = b"second".to_vec();
    let first_digest = sha256_bytes_hex(&first_bytes);
    let second_digest = sha256_bytes_hex(&second_bytes);

    let operations = vec![
        PatchOperation::WriteFile {
            path: "/safe/ok.txt".to_string(),
            content_digest: first_digest.clone(),
            mode: Some(0o644),
        },
        PatchOperation::WriteFile {
            path: "/safe/../escape.txt".to_string(),
            content_digest: second_digest.clone(),
            mode: Some(0o644),
        },
    ];
    let bundle = build_apply_bundle(
        root.path(),
        operations,
        BTreeMap::new(),
        &[
            (first_digest.clone(), first_bytes),
            (second_digest.clone(), second_bytes),
        ],
    );

    let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect_err("path traversal should fail");
    let message = format!("{err:#}");
    assert!(message.contains("operation[1]"));
    assert!(message.contains("failed safety checks"));
    assert!(!root.path().join("safe/ok.txt").exists());
}

#[test]
fn apply_post_state_hash_mismatch_fails() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("opt")).expect("create parent");

    let bytes = b"patched-bytes".to_vec();
    let digest = sha256_bytes_hex(&bytes);
    let operations = vec![PatchOperation::WriteFile {
        path: "/opt/tool".to_string(),
        content_digest: digest.clone(),
        mode: Some(0o755),
    }];
    let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), "f".repeat(64))]);
    let bundle = build_apply_bundle(
        root.path(),
        operations,
        post_state_hashes,
        &[(digest.clone(), bytes)],
    );

    let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect_err("post state hash mismatch should fail");
    let message = format!("{err:#}");
    assert!(message.contains("post-state hash mismatch"));
    assert!(
        !root.path().join("opt/tool").exists(),
        "rollback should restore pre-state"
    );
}

#[test]
fn apply_rolls_back_when_operation_fails_mid_sequence() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("data")).expect("create data");
    fs::write(root.path().join("data/original.txt"), b"original").expect("write original");

    let new_bytes = b"new-data".to_vec();
    let new_digest = sha256_bytes_hex(&new_bytes);
    let operations = vec![
        PatchOperation::WriteFile {
            path: "/data/new.txt".to_string(),
            content_digest: new_digest.clone(),
            mode: Some(0o644),
        },
        PatchOperation::DeleteFile {
            path: "/data/original.txt".to_string(),
        },
        PatchOperation::SetMode {
            path: "/data/missing.txt".to_string(),
            mode: 0o644,
        },
    ];
    let bundle = build_apply_bundle(
        root.path(),
        operations,
        BTreeMap::new(),
        &[(new_digest, new_bytes)],
    );

    let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect_err("mid-sequence failure should rollback");
    let message = format!("{err:#}");
    assert!(message.contains("operation[2]"));

    assert_eq!(
        fs::read(root.path().join("data/original.txt")).expect("original restored"),
        b"original"
    );
    assert!(!root.path().join("data/new.txt").exists());
}

#[test]
fn apply_operation_error_includes_index_and_path() {
    let root = tempdir().expect("create root");
    let patch_state_path = root.path().join("patch-state.json");
    fs::create_dir_all(root.path().join("etc")).expect("create etc");

    let operations = vec![PatchOperation::SetMode {
        path: "/etc/does-not-exist".to_string(),
        mode: 0o644,
    }];
    let bundle = build_apply_bundle(root.path(), operations, BTreeMap::new(), &[]);

    let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
        .expect_err("missing file should fail");
    let message = format!("{err:#}");
    assert!(message.contains("operation[0]"));
    assert!(message.contains("/etc/does-not-exist"));
}

#[test]
fn image_delta_roundtrip_matches_target() {
    let dir = tempdir().expect("create temp dir");
    let base = dir.path().join("base.img");
    let target = dir.path().join("target.img");
    let delta = dir.path().join("patch.vzdelta");
    let output = dir.path().join("output.img");

    let mut base_bytes = vec![0u8; 1024 * 1024];
    for (idx, byte) in base_bytes.iter_mut().enumerate() {
        *byte = (idx % 251) as u8;
    }
    let mut target_bytes = base_bytes.clone();
    target_bytes[16_384..16_384 + 1024].fill(0xAA);
    target_bytes[512_000..512_000 + 2048].fill(0x55);
    target_bytes.extend_from_slice(b"tail-bytes");

    fs::write(&base, &base_bytes).expect("write base");
    fs::write(&target, &target_bytes).expect("write target");
    write_test_image_sidecars(&base);

    let header = create_image_delta_file(&base, &target, &delta, 128 * 1024).expect("create delta");
    assert!(header.changed_chunks > 0);
    let applied_header =
        apply_image_delta_file(&base, &delta, &output).expect("apply delta should succeed");
    assert_eq!(applied_header, header);
    assert_eq!(fs::read(&output).expect("read output"), target_bytes);
}

#[test]
fn image_delta_apply_rejects_base_digest_mismatch() {
    let dir = tempdir().expect("create temp dir");
    let base = dir.path().join("base.img");
    let target = dir.path().join("target.img");
    let delta = dir.path().join("patch.vzdelta");
    let output = dir.path().join("output.img");

    fs::write(&base, b"base-original").expect("write base");
    fs::write(&target, b"base-modified").expect("write target");
    write_test_image_sidecars(&base);
    create_image_delta_file(&base, &target, &delta, 64 * 1024).expect("create delta");

    fs::write(&base, b"base-tampered").expect("tamper base");
    let err = apply_image_delta_file(&base, &delta, &output)
        .expect_err("tampered base must fail digest check");
    assert!(format!("{err:#}").contains("base image digest mismatch"));
}

#[test]
fn image_delta_apply_rejects_existing_output_path() {
    let dir = tempdir().expect("create temp dir");
    let base = dir.path().join("base.img");
    let target = dir.path().join("target.img");
    let delta = dir.path().join("patch.vzdelta");
    let output = dir.path().join("output.img");

    fs::write(&base, b"abc").expect("write base");
    fs::write(&target, b"abd").expect("write target");
    write_test_image_sidecars(&base);
    fs::write(&output, b"existing").expect("write existing output");
    create_image_delta_file(&base, &target, &delta, 64 * 1024).expect("create delta");

    let err =
        apply_image_delta_file(&base, &delta, &output).expect_err("existing output should fail");
    assert!(format!("{err:#}").contains("output image already exists"));
}

#[test]
fn image_delta_apply_rejects_missing_required_sidecar() {
    let dir = tempdir().expect("create temp dir");
    let base = dir.path().join("base.img");
    let target = dir.path().join("target.img");
    let delta = dir.path().join("patch.vzdelta");
    let output = dir.path().join("output.img");

    fs::write(&base, b"abc").expect("write base");
    fs::write(&target, b"abd").expect("write target");
    write_test_image_sidecars(&base);
    fs::remove_file(base.with_extension("machineid")).expect("remove machineid sidecar");
    create_image_delta_file(&base, &target, &delta, 64 * 1024).expect("create delta");

    let err =
        apply_image_delta_file(&base, &delta, &output).expect_err("missing sidecar should fail");
    assert!(format!("{err:#}").contains("required image sidecar not found"));
}
