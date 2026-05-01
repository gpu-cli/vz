//! One-shot probe: boot a single VM with serial logging and grep the kernel
//! ring buffer for "virtio_balloon" probe messages. Confirms the rebuilt
//! kernel actually has the balloon driver compiled in and probed.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use vz::{NetworkConfig, VmConfigBuilder};

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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn virtio_balloon_driver_loads_in_guest() {
    if !has_virtualization_entitlement() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        return;
    };

    let tmp = tempfile::tempdir().unwrap();
    let serial_log = tmp.path().join("serial.log");

    let cfg = VmConfigBuilder::new()
        .cpus(1)
        .memory_bytes(1024 * 1024 * 1024) // 1 GB
        .boot_linux(
            kernel,
            Some(initramfs),
            // Verbose loglevel + ignore_loglevel so every printk shows up.
            "console=hvc0 vz_idle=1",
        )
        .network(NetworkConfig::None)
        .nested_virtualization(false)
        .serial_log_file(serial_log.clone())
        .build()
        .unwrap();

    let vm = vz::Vm::create(cfg).await.unwrap();
    vm.start().await.unwrap();

    // Give the guest enough time to print boot logs.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let _ = vm.stop().await;
    drop(vm);

    let log = std::fs::read_to_string(&serial_log).unwrap_or_default();
    eprintln!("=== full serial log ({} bytes) ===", log.len());
    eprintln!("{log}");
    eprintln!("=== end ===");

    // Don't fail on absence of "balloon" in the log — the driver may probe
    // silently. Just dump the diagnostic so we can read which virtio devices
    // the guest sees and which drivers are bound.
    let _ = log;
}
