//! Layer 1: VmState tests.
//!
//! Tests state type properties: equality, cloning, debug output.

#![allow(clippy::unwrap_used)]

use vz::VmState;

// ---------------------------------------------------------------------------
// Equality
// ---------------------------------------------------------------------------

#[test]
fn stopped_equals_stopped() {
    assert_eq!(VmState::Stopped, VmState::Stopped);
}

#[test]
fn running_equals_running() {
    assert_eq!(VmState::Running, VmState::Running);
}

#[test]
fn error_states_with_same_message_are_equal() {
    assert_eq!(VmState::Error("boom".into()), VmState::Error("boom".into()));
}

#[test]
fn error_states_with_different_messages_differ() {
    assert_ne!(
        VmState::Error("boom".into()),
        VmState::Error("crash".into())
    );
}

#[test]
fn different_states_are_not_equal() {
    assert_ne!(VmState::Stopped, VmState::Running);
    assert_ne!(VmState::Running, VmState::Paused);
    assert_ne!(VmState::Pausing, VmState::Paused);
    assert_ne!(VmState::Starting, VmState::Running);
    assert_ne!(VmState::Stopping, VmState::Stopped);
    assert_ne!(VmState::Saving, VmState::Restoring);
}

// ---------------------------------------------------------------------------
// Clone
// ---------------------------------------------------------------------------

#[test]
fn clone_preserves_value() {
    let states = vec![
        VmState::Stopped,
        VmState::Starting,
        VmState::Running,
        VmState::Pausing,
        VmState::Paused,
        VmState::Resuming,
        VmState::Stopping,
        VmState::Saving,
        VmState::Restoring,
        VmState::Error("test error".into()),
    ];
    for state in &states {
        let cloned = state.clone();
        assert_eq!(state, &cloned);
    }
}

// ---------------------------------------------------------------------------
// Debug
// ---------------------------------------------------------------------------

#[test]
fn debug_output_for_all_variants() {
    assert_eq!(format!("{:?}", VmState::Stopped), "Stopped");
    assert_eq!(format!("{:?}", VmState::Starting), "Starting");
    assert_eq!(format!("{:?}", VmState::Running), "Running");
    assert_eq!(format!("{:?}", VmState::Pausing), "Pausing");
    assert_eq!(format!("{:?}", VmState::Paused), "Paused");
    assert_eq!(format!("{:?}", VmState::Resuming), "Resuming");
    assert_eq!(format!("{:?}", VmState::Stopping), "Stopping");
    assert_eq!(format!("{:?}", VmState::Saving), "Saving");
    assert_eq!(format!("{:?}", VmState::Restoring), "Restoring");

    let error_debug = format!("{:?}", VmState::Error("something broke".into()));
    assert!(error_debug.contains("Error"));
    assert!(error_debug.contains("something broke"));
}

// ---------------------------------------------------------------------------
// Pattern matching coverage
// ---------------------------------------------------------------------------

#[test]
fn all_variants_are_exhaustive() {
    // This test verifies at compile time that our match covers all variants.
    let state = VmState::Stopped;
    let label = match state {
        VmState::Stopped => "stopped",
        VmState::Starting => "starting",
        VmState::Running => "running",
        VmState::Pausing => "pausing",
        VmState::Paused => "paused",
        VmState::Resuming => "resuming",
        VmState::Stopping => "stopping",
        VmState::Saving => "saving",
        VmState::Restoring => "restoring",
        VmState::Error(_) => "error",
    };
    assert_eq!(label, "stopped");
}
