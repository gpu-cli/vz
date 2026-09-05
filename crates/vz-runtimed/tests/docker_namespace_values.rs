#[path = "support/docker_namespace_values.rs"]
mod values;

use values::{container_id, time_namespace};

#[test]
fn cleanup_ids_are_full_lowercase_hex_only() {
    assert!(container_id(&"a".repeat(64)).is_ok());
    for invalid in [
        "abc",
        "--all",
        "../../other",
        &"A".repeat(64),
        &format!("{} other", "a".repeat(64)),
    ] {
        assert!(container_id(invalid).is_err());
    }
}

#[test]
fn namespace_observations_must_be_exact_nonzero_time_inodes() {
    assert!(time_namespace("time:[1234]\n").is_ok());
    for invalid in [
        "pid:[1234]",
        "time:[0]",
        "time:[012]",
        "time:[]",
        "time:[12] noise",
        "time:[-1]",
    ] {
        assert!(time_namespace(invalid).is_err());
    }
}
