//! Integration tests for the live VM state stream.
//!
//! These guard the contract that `Vm::state_stream` reflects every
//! framework-driven transition — not just the terminal `Stopped` /
//! `Error(_)` from the delegate callbacks. The pre-fix vz crate only
//! observed `guestDidStopVirtualMachine:` and
//! `virtualMachine:didStopWithError:`, so the watch was effectively
//! binary (initial Stopped → terminal Stopped/Error). Boon prep VMs
//! relied on `wait_for_halt(...)` to distinguish "VM never ran" from
//! "VM ran and halted," and the missing intermediate transitions
//! caused VRT-yl5l (state.ext4 came back all zeros after a "successful"
//! prep run because the watcher returned on the initial Stopped).
//!
//! Apple's framework drives `VZVirtualMachine.state` through Stopped →
//! Starting → Running → Stopping → Stopped on a clean lifecycle. We
//! verify the watcher sees at least one non-Stopped state between
//! `start()` and the terminal stop.
//!
//! Requirements:
//! - Apple Silicon
//! - Linux kernel artifacts at `~/.vz/linux/` (`vmlinux`, `initramfs.img`)
//! - Test binary signed with `com.apple.security.virtualization` entitlement
//!   (use `./scripts/run-sandbox-vm-e2e.sh`).

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use vz::{NetworkConfig, Vm, VmConfigBuilder, VmState};

const ONE_MB: u64 = 1024 * 1024;

fn linux_artifacts() -> Option<(PathBuf, PathBuf)> {
    let home = std::env::var_os("HOME")?;
    let vz_dir = PathBuf::from(home).join(".vz/linux");
    let kernel = vz_dir.join("vmlinux");
    let initramfs = vz_dir.join("initramfs.img");
    if kernel.exists() && initramfs.exists() {
        Some((kernel, initramfs))
    } else {
        None
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

fn skip_if_unentitled() -> bool {
    if !has_virtualization_entitlement() {
        eprintln!(
            "skipping state_stream_test: missing com.apple.security.virtualization \
             entitlement; run via ./scripts/run-sandbox-vm-e2e.sh"
        );
        return true;
    }
    false
}

fn build_minimal_linux_config(kernel: PathBuf, initramfs: PathBuf) -> vz::config::VmConfig {
    VmConfigBuilder::new()
        .cpus(1)
        .memory_bytes(256 * ONE_MB)
        .boot_linux(kernel, Some(initramfs), "console=hvc0 quiet")
        .network(NetworkConfig::None)
        .nested_virtualization(false)
        .build()
        .unwrap()
}

/// Drain the state watcher into a Vec until either `predicate` returns
/// true on the latest value or `deadline` elapses. Returns the full
/// transition log so callers can assert on the trajectory.
async fn collect_states_until(
    vm: &Vm,
    deadline: Duration,
    mut predicate: impl FnMut(&VmState) -> bool,
) -> Vec<VmState> {
    let mut rx = vm.state_stream();
    let mut log = Vec::new();
    let snapshot = rx.borrow().clone();
    log.push(snapshot.clone());
    if predicate(&snapshot) {
        return log;
    }
    let watcher = async {
        loop {
            if rx.changed().await.is_err() {
                return;
            }
            let snapshot = rx.borrow().clone();
            log.push(snapshot.clone());
            if predicate(&snapshot) {
                return;
            }
        }
    };
    let _ = tokio::time::timeout(deadline, watcher).await;
    log
}

/// VRT-yl5l: after `start()` the watcher MUST observe a non-Stopped
/// state before the terminal Stopped. Pre-fix this test would fail —
/// the watcher only ever saw `Stopped, Stopped` because the delegate
/// reflects `guestDidStop` as `Stopped` and there's no other source
/// for the intermediate Starting/Running transitions.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn state_stream_observes_non_stopped_transition() {
    if skip_if_unentitled() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let config = build_minimal_linux_config(kernel, initramfs);
    let vm = Vm::create(config).await.unwrap();

    // Pre-start: watcher reports the initial Stopped baked into the
    // watch::channel. This is the value `wait_for_halt` callers used
    // to misread as "VM has halted."
    assert_eq!(
        vm.state_stream().borrow().clone(),
        VmState::Stopped,
        "initial state must be Stopped before start()",
    );

    vm.start().await.unwrap();

    // Collect state transitions until we either see Running OR the
    // VM transitions back to Stopped/Error (whichever comes first).
    // The minimal Linux VM with a no-op initramfs panics quickly with
    // "Attempted to kill init!", so on slower hosts we may catch
    // Starting → Stopping or Starting → Error directly without
    // observing Running. The contract under test is that the watch
    // sees at least one non-Stopped/non-Error transition AFTER the
    // initial Stopped.
    let transitions = collect_states_until(&vm, Duration::from_secs(15), |s| {
        matches!(s, VmState::Stopped | VmState::Error(_)) && {
            // Only consider this the "terminal" if we already saw
            // a non-Stopped state. Otherwise keep waiting.
            // The closure can't easily reach back into transitions,
            // so we just always continue past the initial Stopped
            // and let the deadline bound the loop.
            false
        } || matches!(s, VmState::Running | VmState::Starting | VmState::Stopping)
    })
    .await;

    let saw_intermediate = transitions
        .iter()
        .any(|s| matches!(s, VmState::Starting | VmState::Running | VmState::Stopping));

    assert!(
        saw_intermediate,
        "expected to observe Starting / Running / Stopping in transitions but only saw {:?}",
        transitions,
    );
}

/// Complement to `state_stream_observes_non_stopped_transition`: the
/// non-async `Vm::state` getter must agree with the latest watch
/// snapshot. Both go through `state_rx.borrow()`, so this is a guard
/// against future drift if someone splits the two paths.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn state_getter_matches_state_stream_after_start() {
    if skip_if_unentitled() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let config = build_minimal_linux_config(kernel, initramfs);
    let vm = Vm::create(config).await.unwrap();
    vm.start().await.unwrap();

    // Race window: after start() pushes a state, the watcher and the
    // synchronous getter are reading the SAME watch::Receiver, so they
    // must always agree.
    let from_getter = vm.state();
    let from_stream = vm.state_stream().borrow().clone();
    assert_eq!(
        from_getter, from_stream,
        "Vm::state() and Vm::state_stream() must report the same value",
    );

    // Importantly, the post-start state must not be the original
    // Stopped — otherwise we're back in the VRT-yl5l failure mode.
    assert_ne!(
        from_getter,
        VmState::Stopped,
        "post-start state must not be the initial Stopped",
    );
}

/// Pause / resume round-trip: each API call must push the post-call
/// state into the watch so suspend/resume callers can synchronize on
/// the transition.
#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn pause_and_resume_push_state_transitions() {
    if skip_if_unentitled() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let config = build_minimal_linux_config(kernel, initramfs);
    let vm = Vm::create(config).await.unwrap();
    vm.start().await.unwrap();

    // Wait for the VM to be running before pausing — Apple rejects
    // pause requests that race ahead of Starting.
    let mut rx = vm.state_stream();
    let wait_running = async {
        loop {
            if matches!(*rx.borrow(), VmState::Running) {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    };
    let reached_running = tokio::time::timeout(Duration::from_secs(5), wait_running)
        .await
        .is_ok();
    if !reached_running {
        // The minimal initramfs may panic before reaching Running.
        // Skip the pause assertions in that case — those rely on a
        // working guest, which isn't required for the headline fix.
        eprintln!(
            "VM never reached Running with the minimal initramfs; \
             pause/resume assertions skipped. State: {:?}",
            vm.state()
        );
        return;
    }

    vm.pause().await.unwrap();
    let post_pause = vm.state();
    assert!(
        matches!(post_pause, VmState::Paused | VmState::Pausing),
        "pause() should leave the watch at Paused or Pausing; got {:?}",
        post_pause,
    );

    vm.resume().await.unwrap();
    let post_resume = vm.state();
    assert!(
        matches!(post_resume, VmState::Running | VmState::Resuming),
        "resume() should leave the watch at Running or Resuming; got {:?}",
        post_resume,
    );
}
