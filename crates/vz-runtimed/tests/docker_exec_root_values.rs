//! Synthetic kernel observations, never physical Docker certification.
#![allow(clippy::unwrap_used)]
#[path = "support/docker_exec_root_values.rs"]
mod values;

const GUEST: &str =
    "35:917575\n26:2\npid:[4026532231]\nmnt:[4026532305]\npid:[4026531836]\nmnt:[4026531841]\n";
const EXEC: &str = "35:917575\n35:917575\npid:[4026532231]\nmnt:[4026532305]\npid:[4026532231]\nmnt:[4026532305]\n";

#[test]
fn validates_exact_root_and_namespace_observations() {
    assert!(values::validate(GUEST, EXEC, GUEST).is_ok());
    assert!(values::EXEC_SCRIPT.contains("/proc/1/ns/pid"));
    assert!(
        values::guest_script(1051)
            .unwrap()
            .contains("/proc/1051/root")
    );
}

#[test]
fn rejects_observed_broader_root_even_with_correct_namespaces() {
    assert!(values::validate(GUEST, &EXEC.replacen("35:917575", "2:1", 1), GUEST).is_err());
}

#[test]
fn rejects_wrong_proc_view_and_changed_or_shared_namespaces() {
    for bad in [
        EXEC.replacen("pid:[4026532231]", "pid:[4026531836]", 1),
        EXEC.replace("mnt:[4026532305]", "mnt:[4026531841]"),
        EXEC.replacen("pid:[4026532231]", "pid:[1]", 2),
    ] {
        assert!(values::validate(GUEST, &bad, GUEST).is_err());
    }
    assert!(values::validate(GUEST, EXEC, &GUEST.replace("917575", "917576")).is_err());
    let shared = GUEST.replace("26:2", "35:917575");
    assert!(values::validate(&shared, EXEC, &shared).is_err());
}

#[test]
fn rejects_malformed_and_overflowed_kernel_identities() {
    for bad in [
        "",
        "35:0",
        "035:917575",
        "35:01",
        "-1:2",
        "35:18446744073709551616",
        "35:2:3",
    ] {
        let guest = GUEST.replace("35:917575", bad);
        let exec = EXEC.replace("35:917575", bad);
        assert!(values::validate(&guest, &exec, &guest).is_err());
    }
    for bad in [
        "pid:[0]",
        "pid:[01]",
        "time:[4026532231]",
        "pid:[18446744073709551616]",
    ] {
        let guest = GUEST.replace("pid:[4026532231]", bad);
        let exec = EXEC.replace("pid:[4026532231]", bad);
        assert!(values::validate(&guest, &exec, &guest).is_err());
    }
    assert!(values::validate(GUEST, &(EXEC.to_string() + "extra\n"), GUEST).is_err());
    assert!(values::validate(GUEST, &"x".repeat(1025), GUEST).is_err());
    for bad in [0, 1, i32::MAX as u64 + 1, u64::MAX] {
        assert!(values::guest_script(bad).is_err());
    }
}
