//! Memory balloon runtime control end-to-end.
//!
//! Boots a minimal Linux VM, exercises `Vm::target_memory_size` /
//! `Vm::set_target_memory_size`, and asserts the API contract holds
//! against Apple's framework. Does not need a guest agent — only the
//! host-side balloon device property is read/written.
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

use vz::{NetworkConfig, VmConfigBuilder};

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
            "skipping balloon_test: missing com.apple.security.virtualization entitlement; \
             run via ./scripts/run-sandbox-vm-e2e.sh"
        );
        return true;
    }
    false
}

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn balloon_default_on_target_size_roundtrip() {
    if skip_if_unentitled() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let memory_bytes = 1024 * ONE_MB; // 1 GB

    let config = VmConfigBuilder::new()
        .cpus(1)
        .memory_bytes(memory_bytes)
        .boot_linux(kernel, Some(initramfs), "console=hvc0 quiet")
        .network(NetworkConfig::None)
        .nested_virtualization(false)
        .build()
        .unwrap();

    assert!(
        config.memory_balloon_enabled(),
        "default builder should enable the balloon"
    );

    let vm = vz::Vm::create(config).await.unwrap();
    vm.start().await.unwrap();

    // Apple initializes targetVirtualMachineMemorySize to the configured memory.
    let initial = vm.target_memory_size().await.unwrap();
    assert_eq!(
        initial, memory_bytes,
        "initial target should equal configured memory"
    );

    // Halve it. Apple rounds down to 1 MB and clamps to [min, max].
    let target = memory_bytes / 2;
    vm.set_target_memory_size(target).await.unwrap();

    // Brief wait — Apple's setter is fire-and-forget but the property
    // should reflect immediately after the dispatch returns.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let observed = vm.target_memory_size().await.unwrap();
    let expected = (target / ONE_MB) * ONE_MB;
    assert_eq!(
        observed, expected,
        "target should round down to 1 MB alignment"
    );

    vm.stop().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn balloon_disabled_makes_runtime_calls_fail() {
    if skip_if_unentitled() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let config = VmConfigBuilder::new()
        .cpus(1)
        .memory_bytes(512 * ONE_MB)
        .boot_linux(kernel, Some(initramfs), "console=hvc0 quiet")
        .network(NetworkConfig::None)
        .nested_virtualization(false)
        .memory_balloon(false)
        .build()
        .unwrap();

    assert!(!config.memory_balloon_enabled());

    let vm = vz::Vm::create(config).await.unwrap();
    vm.start().await.unwrap();

    let read_err = vm.target_memory_size().await.unwrap_err();
    assert!(
        format!("{read_err}").contains("memory balloon not enabled"),
        "unexpected error reading balloon when disabled: {read_err}"
    );

    let write_err = vm.set_target_memory_size(256 * ONE_MB).await.unwrap_err();
    assert!(
        format!("{write_err}").contains("memory balloon not enabled"),
        "unexpected error writing balloon when disabled: {write_err}"
    );

    vm.stop().await.unwrap();
}
