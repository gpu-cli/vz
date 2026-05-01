//! Verify the guest balloon driver actually inflates when the host calls
//! `set_target_memory_size`. Boots a single VM, asks initramfs to dump
//! /proc/meminfo to serial every 2 seconds via `vz_idle=1`, halves the
//! target memory size, and asserts the guest-side `MemAvailable` drops by
//! roughly the right amount.
//!
//! This isolates the question "does the balloon actually inflate" from the
//! question "does the host reclaim pages" — the former is provable from
//! guest-side numbers alone.

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

/// Parse the most recent `MemAvailable=NkB` value from the serial log.
fn last_mem_available_kb(log: &str) -> Option<u64> {
    log.lines()
        .filter_map(|line| {
            let idx = line.find("MemAvailable=")?;
            let rest = &line[idx + "MemAvailable=".len()..];
            let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            num.parse::<u64>().ok()
        })
        .last()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn balloon_inflate_drops_guest_mem_available() {
    if !has_virtualization_entitlement() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        return;
    };

    let tmp = tempfile::tempdir().unwrap();
    let serial_log = tmp.path().join("serial.log");
    let memory_bytes = 2 * 1024 * 1024 * 1024; // 2 GB

    let cfg = VmConfigBuilder::new()
        .cpus(1)
        .memory_bytes(memory_bytes)
        .boot_linux(kernel, Some(initramfs), "console=hvc0 vz_idle=1")
        .network(NetworkConfig::None)
        .nested_virtualization(false)
        .serial_log_file(serial_log.clone())
        .build()
        .unwrap();

    let vm = vz::Vm::create(cfg).await.unwrap();
    vm.start().await.unwrap();

    // Let boot settle and the meminfo poll get a few samples.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let log_before = std::fs::read_to_string(&serial_log).unwrap_or_default();
    let before_kb = last_mem_available_kb(&log_before).expect(
        "expected at least one MemAvailable sample before balloon — check vz_idle init path",
    );
    eprintln!("guest MemAvailable BEFORE balloon: {} kB", before_kb);

    // Balloon to 1/4 of configured. Apple's setter rounds to 1 MB and clamps.
    let target = memory_bytes / 4;
    vm.set_target_memory_size(target).await.unwrap();
    eprintln!("set_target_memory_size({} MB) sent", target / (1024 * 1024));

    // Give the balloon driver real time to inflate. Allocations of ~1.5 GB
    // can be slow on a single CPU.
    tokio::time::sleep(Duration::from_secs(30)).await;

    let log_after = std::fs::read_to_string(&serial_log).unwrap_or_default();
    let after_kb = last_mem_available_kb(&log_after).expect(
        "expected at least one MemAvailable sample after balloon",
    );
    eprintln!("guest MemAvailable AFTER  balloon: {} kB", after_kb);

    // Print the trailing meminfo lines for the human reader.
    eprintln!("=== last 8 [vz-meminfo] lines ===");
    let meminfo_lines: Vec<&str> = log_after
        .lines()
        .filter(|l| l.contains("[vz-meminfo]"))
        .collect();
    for line in meminfo_lines.iter().rev().take(8).rev() {
        eprintln!("{line}");
    }

    let _ = vm.stop().await;

    // Expectation: ballooning to 512 MB target on a 2 GB VM should drop
    // guest MemAvailable by at least 1 GB (1_000_000 kB ish).
    let drop_kb = before_kb.saturating_sub(after_kb);
    eprintln!(
        "MemAvailable drop: {} kB ({} MB)",
        drop_kb,
        drop_kb / 1024
    );
    assert!(
        drop_kb > 500_000, // > ~500 MB
        "expected balloon to inflate by > 500 MB, observed only {} kB drop",
        drop_kb
    );
}
