#[path = "support/docker_device_policy_values.rs"]
mod values;

use serde_json::json;
use values::{DENIED_OPEN, capability_status, denied_open};

#[test]
fn capability_evidence_requires_effective_mknod_and_active_seccomp() -> anyhow::Result<()> {
    assert_eq!(
        capability_status("CapEff:\t0000000008000000\nSeccomp:\t2\n")?,
        (1 << 27, 2)
    );
    for invalid in [
        "CapEff:\t0000000000000000\nSeccomp:\t2\n",
        "CapEff:\t0000000008000000\nSeccomp:\t0\n",
        "CapEff:\t0000000008000000\n",
        "CapEff:\t0000000008000000\nCapEff:\t0000000008000000\nSeccomp:\t2\n",
        "CapEff:\t8000000\nSeccomp:\t2\n",
    ] {
        assert!(capability_status(invalid).is_err(), "accepted {invalid:?}");
    }
    Ok(())
}

#[test]
fn denied_open_requires_exact_errno_text_not_any_failure() {
    let receipt = json!({"exit_code": 1, "stdout": "", "stderr": DENIED_OPEN});
    assert!(denied_open(&receipt).is_ok());
    for (field, value) in [
        ("exit_code", json!(0)),
        ("exit_code", json!(125)),
        ("stdout", json!("unexpected")),
        (
            "stderr",
            json!("dd: can't open '/dev/vz-policy-loop-control': Permission denied\n"),
        ),
        (
            "stderr",
            json!("dd: can't open '/dev/vz-policy-loop-control': No such device\n"),
        ),
        ("stderr", json!(format!("{DENIED_OPEN}another failure\n"))),
    ] {
        let mut changed = receipt.clone();
        changed[field] = value;
        assert!(denied_open(&changed).is_err(), "accepted {changed}");
    }
}
