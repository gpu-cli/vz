#![cfg(all(target_os = "macos", feature = "e2e-test-hooks"))]
#![allow(clippy::expect_used)]

use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};

#[path = "support/committed_json.rs"]
mod committed_json;
use committed_json::{
    json_commit_path, read_committed_json, try_write_new_file_atomically, write_committed_json,
    write_json,
};

#[tokio::test]
async fn committed_json_handoff_publishes_one_complete_checksum_bound_payload() {
    let root = tempfile::tempdir().expect("committed JSON tempdir");
    let path = root.path().join("identity.json");
    let expected = json!({
        "schema_version": 1,
        "stack_id": "handoff",
        "incarnation_id": "00000000-0000-4000-8000-000000000001"
    });

    let payload = serde_json::to_vec_pretty(&expected).expect("serialize test identity");
    try_write_new_file_atomically(&path, &payload, |temporary| {
        assert!(!path.exists(), "final path was visible before publication");
        assert_eq!(
            std::fs::read(temporary).expect("read synced temporary"),
            payload,
            "temporary payload was incomplete before publication"
        );
    })
    .expect("publish test identity");
    let commit_path = json_commit_path(&path);
    let checksum = format!("{:x}", Sha256::digest(&payload));
    try_write_new_file_atomically(&commit_path, checksum.as_bytes(), |temporary| {
        assert!(path.is_file(), "payload was absent before commit");
        assert!(
            !commit_path.exists(),
            "commit path was visible before publication"
        );
        assert_eq!(
            std::fs::read_to_string(temporary).expect("read commit temporary"),
            checksum
        );
    })
    .expect("publish test identity commit");
    let decoded: serde_json::Value = read_committed_json(&path, Duration::from_secs(1)).await;
    assert_eq!(decoded, expected);
    assert!(path.is_file());
    assert!(commit_path.is_file());

    let existing = root.path().join("existing.json");
    std::fs::write(&existing, b"original").expect("seed existing destination");
    let error = try_write_new_file_atomically(&existing, b"replacement", |_| {})
        .expect_err("atomic publication replaced an existing destination");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        std::fs::read(&existing).expect("read existing destination"),
        b"original"
    );

    let malformed = root.path().join("malformed.json");
    try_write_new_file_atomically(&malformed, b"{", |_| {})
        .expect("publish malformed test payload");
    let malformed_checksum = format!("{:x}", Sha256::digest(b"{"));
    try_write_new_file_atomically(
        &json_commit_path(&malformed),
        malformed_checksum.as_bytes(),
        |_| {},
    )
    .expect("publish malformed test commit");
    let malformed_task = tokio::spawn(async move {
        let _: serde_json::Value = read_committed_json(&malformed, Duration::from_secs(1)).await;
    });
    let join_error = malformed_task
        .await
        .expect_err("malformed committed JSON was accepted");
    assert!(
        join_error.is_panic(),
        "malformed JSON did not fail strictly"
    );

    let wrapper_path = root.path().join("wrapper.json");
    write_committed_json(&wrapper_path, &expected);
    let wrapper: serde_json::Value =
        read_committed_json(&wrapper_path, Duration::from_secs(1)).await;
    assert_eq!(wrapper, expected);
    let plain_path = root.path().join("plain.json");
    write_json(&plain_path, &expected);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(&plain_path).expect("read plain atomic JSON")
        )
        .expect("decode plain atomic JSON"),
        expected
    );
    assert!(
        std::fs::read_dir(root.path())
            .expect("list committed JSON directory")
            .all(|entry| !entry
                .expect("committed JSON directory entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")),
        "atomic handoff left a temporary file"
    );
}
