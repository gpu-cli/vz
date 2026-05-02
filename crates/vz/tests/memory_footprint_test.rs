//! Measure the host RSS cost of running N concurrent vz Linux VMs.
//!
//! What this proves:
//! - When lazy paging works, idle VMs do NOT cost their full configured size.
//!   The host pays for what the guest has actually touched, not what was
//!   declared.
//! - When the memory balloon is wired up (vz-jhr), reducing the target memory
//!   size should reduce the host RSS the framework holds.
//!
//! Methodology: sample test-process RSS via `ps`, since Apple's framework
//! allocates guest physical memory inside the calling process. The number is
//! noisy (other system activity, kernel compaction) so we report a single
//! point measurement rather than precise bounds.
//!
//! Requirements: Apple Silicon, kernel artifacts at ~/.vz/linux, signed
//! test binary with `com.apple.security.virtualization` entitlement.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use vz::{NetworkConfig, VmConfigBuilder};

const ONE_MB: u64 = 1024 * 1024;
const ONE_GB: u64 = 1024 * ONE_MB;

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

/// Test-process RSS in bytes. Reads via `ps -o rss=` which on macOS reports KB.
fn proc_rss_bytes() -> u64 {
    let pid = std::process::id();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let kb: u64 = stdout.trim().parse().expect("rss kb");
    kb * 1024
}

/// Per-VM helper-process accounting.
///
/// Apple's Virtualization.framework runs each VM in its own XPC service
/// process (`com.apple.Virtualization.VirtualMachine.xpc`), launched by
/// launchd with PPID=1 — NOT as a child of the calling process. The guest's
/// physical memory lives in that helper, so the test process's own RSS is a
/// red herring for "host cost of N VMs." We have to walk the process table
/// for the helpers and sum their RSS to get the real number.
#[derive(Debug, Default, Clone)]
struct HelperSnapshot {
    count: usize,
    total_rss_bytes: u64,
    total_vsz_bytes: u64,
    pids: Vec<u32>,
}

fn helper_processes() -> HelperSnapshot {
    // `ps -axo pid,rss,vsz,comm` — match the framework's XPC service comm.
    let output = Command::new("ps")
        .args(["-axo", "pid=,rss=,vsz=,comm="])
        .output()
        .expect("ps");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut snap = HelperSnapshot::default();
    for line in stdout.lines() {
        if !line.contains("com.apple.Virtualization.VirtualMachine") {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(pid) = parts.next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        let Some(rss_kb) = parts.next().and_then(|p| p.parse::<u64>().ok()) else {
            continue;
        };
        let Some(vsz_kb) = parts.next().and_then(|p| p.parse::<u64>().ok()) else {
            continue;
        };
        snap.count += 1;
        snap.total_rss_bytes += rss_kb * 1024;
        snap.total_vsz_bytes += vsz_kb * 1024;
        snap.pids.push(pid);
    }
    snap
}

fn fmt_mb(bytes: u64) -> String {
    format!("{} MB", bytes / ONE_MB)
}

/// Boot `n` VMs configured at `memory_per_vm` bytes with the supplied kernel
/// cmdline, wait `idle_secs` for boot to settle, and return `(rss_delta_bytes,
/// helper_count, vsz_delta_bytes)` where `rss_delta_bytes` is the increase in
/// the sum of `com.apple.Virtualization.VirtualMachine.xpc` helper RSS over
/// what it was before the VMs were created.
///
/// Stops + drops the VMs before returning. Sleeps an extra 3s after stop to
/// let helpers exit before the caller takes its next baseline.
async fn measure_n_vms(
    n: usize,
    memory_per_vm: u64,
    cmdline: &str,
    idle_secs: u64,
    kernel: &PathBuf,
    initramfs: &PathBuf,
) -> (u64, usize, u64) {
    let baseline = helper_processes();
    let mut vms = Vec::with_capacity(n);
    for _ in 0..n {
        let cfg = VmConfigBuilder::new()
            .cpus(1)
            .memory_bytes(memory_per_vm)
            .boot_linux(kernel.clone(), Some(initramfs.clone()), cmdline)
            .network(NetworkConfig::None)
            .nested_virtualization(false)
            .build()
            .unwrap();
        let vm = vz::Vm::create(cfg).await.unwrap();
        vm.start().await.unwrap();
        vms.push(vm);
    }
    tokio::time::sleep(Duration::from_secs(idle_secs)).await;
    let after = helper_processes();
    let rss_delta = after
        .total_rss_bytes
        .saturating_sub(baseline.total_rss_bytes);
    let vsz_delta = after
        .total_vsz_bytes
        .saturating_sub(baseline.total_vsz_bytes);
    let new_helpers = after.count.saturating_sub(baseline.count);

    for vm in &vms {
        let _ = vm.stop().await;
    }
    drop(vms);
    tokio::time::sleep(Duration::from_secs(3)).await;
    (rss_delta, new_helpers, vsz_delta)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn memory_size_sweep_proves_lazy_paging() {
    if !has_virtualization_entitlement() {
        eprintln!("skipping: missing virtualization entitlement");
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    // Sweep configured memory per VM with N=5 to keep total bounded.
    // If lazy paging is working, per-VM cost should be roughly constant
    // (kernel image + initramfs + framework state) regardless of how much
    // memory we declared. If we see linear growth with configured, the
    // framework is eagerly allocating and consumers should worry.
    let n = 5usize;
    let sizes = [512 * ONE_MB, ONE_GB, 2 * ONE_GB, 4 * ONE_GB];
    let cmdline = "console=hvc0 quiet";

    eprintln!();
    eprintln!("==== Memory size sweep (N={n}, idle for 5s, idle cmdline) ====");
    eprintln!(
        "{:>14} {:>10} {:>14} {:>14} {:>10}",
        "configured/VM", "helpers", "sum RSS delta", "RSS/VM", "RSS/cfg"
    );
    for &mem in &sizes {
        let (rss_delta, helpers, _vsz) =
            measure_n_vms(n, mem, cmdline, 5, &kernel, &initramfs).await;
        eprintln!(
            "{:>11} MB {:>10} {:>11} MB {:>11} MB {:>9.1}%",
            mem / ONE_MB,
            helpers,
            rss_delta / ONE_MB,
            rss_delta / (n as u64).max(1) / ONE_MB,
            100.0 * rss_delta as f64 / ((n as u64) * mem) as f64,
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn balloon_reclaims_real_pages_under_load() {
    if !has_virtualization_entitlement() {
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        return;
    };

    // Force the guest to actually allocate physical pages at boot via the
    // initramfs `memhog_mb=` knob. 512 MB of dd-into-tmpfs gives the balloon
    // something concrete to reclaim. `vz_idle=1` keeps init from chaining
    // into the full guest-agent + youki setup so memory accounting stays
    // simple.
    let memory_per_vm = 2 * ONE_GB;
    let n = 4usize;
    let cmdline = "console=hvc0 quiet memhog_mb=512 vz_idle=1";

    let baseline = helper_processes();
    eprintln!();
    eprintln!("==== Balloon reclaim under load ====");
    eprintln!("N VMs:               {n}");
    eprintln!("memory per VM:       {}", fmt_mb(memory_per_vm));
    eprintln!("cmdline:             {cmdline}  (forces 512 MB allocation in tmpfs)");
    eprintln!(
        "baseline helpers:    {} (rss {})",
        baseline.count,
        fmt_mb(baseline.total_rss_bytes)
    );

    let mut vms = Vec::with_capacity(n);
    for _ in 0..n {
        let cfg = VmConfigBuilder::new()
            .cpus(1)
            .memory_bytes(memory_per_vm)
            .boot_linux(kernel.clone(), Some(initramfs.clone()), cmdline)
            .network(NetworkConfig::None)
            .nested_virtualization(false)
            .build()
            .unwrap();
        let vm = vz::Vm::create(cfg).await.unwrap();
        vm.start().await.unwrap();
        vms.push(vm);
    }

    tokio::time::sleep(Duration::from_secs(8)).await;
    let after_boot = helper_processes();
    let after_boot_delta = after_boot
        .total_rss_bytes
        .saturating_sub(baseline.total_rss_bytes);
    eprintln!(
        "after boot+8s idle:  helpers={} sum RSS delta={} ({} per VM)",
        after_boot.count.saturating_sub(baseline.count),
        fmt_mb(after_boot_delta),
        fmt_mb(after_boot_delta / (n as u64).max(1))
    );

    // Balloon down to 1/4 of configured. Apple's setter is fire-and-forget;
    // give the guest balloon driver time to actually inflate, then sample
    // RSS at intervals so we can see whether the host is gradually reclaiming
    // or whether reclaim never happens at all.
    let target = memory_per_vm / 4;
    for vm in &vms {
        vm.set_target_memory_size(target).await.unwrap();
    }
    eprintln!(
        "balloon target set to {} per VM, sampling RSS over time...",
        fmt_mb(target)
    );
    for wait in [5u64, 15, 30, 60] {
        tokio::time::sleep(Duration::from_secs(if wait == 5 { 5 } else { wait - 5 })).await;
        let snap = helper_processes();
        let delta = snap
            .total_rss_bytes
            .saturating_sub(baseline.total_rss_bytes);
        let signed: i64 = snap.total_rss_bytes as i64 - after_boot.total_rss_bytes as i64;
        eprintln!(
            "  t=+{:>2}s helpers={} sum RSS delta={} ({}{} MB vs after-boot)",
            wait,
            snap.count.saturating_sub(baseline.count),
            fmt_mb(delta),
            if signed >= 0 { "+" } else { "" },
            signed / ONE_MB as i64
        );
    }

    for vm in &vms {
        let _ = vm.stop().await;
    }
    drop(vms);
    tokio::time::sleep(Duration::from_secs(3)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts + virt entitlement"]
async fn measure_n_idle_vms_host_rss() {
    if !has_virtualization_entitlement() {
        eprintln!(
            "skipping: missing com.apple.security.virtualization entitlement; \
             sign the binary with entitlements/vz-cli.entitlements.plist"
        );
        return;
    }
    let Some((kernel, initramfs)) = linux_artifacts() else {
        eprintln!("skipping: no kernel artifacts at ~/.vz/linux");
        return;
    };

    let n: usize = 10;
    let memory_per_vm = ONE_GB; // 1 GB configured per VM
    let configured_total = (n as u64) * memory_per_vm;

    let baseline_helpers = helper_processes();
    let baseline_proc_rss = proc_rss_bytes();
    eprintln!();
    eprintln!("==== Setup ====");
    eprintln!("N VMs:               {n}");
    eprintln!("memory per VM:       {}", fmt_mb(memory_per_vm));
    eprintln!("sum configured:      {}", fmt_mb(configured_total));
    eprintln!("test process RSS:    {}", fmt_mb(baseline_proc_rss));
    eprintln!(
        "pre-existing VM helpers: {} (rss {})",
        baseline_helpers.count,
        fmt_mb(baseline_helpers.total_rss_bytes)
    );
    eprintln!();

    let mut vms = Vec::with_capacity(n);
    for i in 0..n {
        let cfg = VmConfigBuilder::new()
            .cpus(1)
            .memory_bytes(memory_per_vm)
            .boot_linux(
                kernel.clone(),
                Some(initramfs.clone()),
                "console=hvc0 quiet",
            )
            .network(NetworkConfig::None)
            .nested_virtualization(false)
            .build()
            .unwrap();
        let vm = vz::Vm::create(cfg).await.unwrap();
        vm.start().await.unwrap();
        vms.push(vm);
        let helpers = helper_processes();
        eprintln!(
            "[{:>2}] booted: helpers={} sum_rss={}",
            i,
            helpers.count,
            fmt_mb(helpers.total_rss_bytes)
        );
    }

    // Let guests finish kernel boot and quiesce.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let after_boot = helper_processes();
    let after_boot_rss = after_boot
        .total_rss_bytes
        .saturating_sub(baseline_helpers.total_rss_bytes);
    eprintln!();
    eprintln!("==== After 5s idle ====");
    eprintln!("XPC helper count:    {}", after_boot.count);
    eprintln!(
        "sum helper RSS:      {}",
        fmt_mb(after_boot.total_rss_bytes)
    );
    eprintln!(
        "sum helper VSZ:      {}",
        fmt_mb(after_boot.total_vsz_bytes)
    );
    eprintln!(
        "delta vs baseline:   {}  ({} per VM)",
        fmt_mb(after_boot_rss),
        fmt_mb(after_boot_rss / (n as u64).max(1))
    );
    eprintln!(
        "ratio delta/cfg:     {:.1}%  ({} of {} configured)",
        100.0 * after_boot_rss as f64 / configured_total as f64,
        fmt_mb(after_boot_rss),
        fmt_mb(configured_total)
    );

    // Balloon every VM down to half. Apple's setter is fire-and-forget; the
    // guest balloon driver responds asynchronously, so give it real time.
    let target_per_vm = memory_per_vm / 2;
    for vm in &vms {
        vm.set_target_memory_size(target_per_vm).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(10)).await;

    let after_balloon = helper_processes();
    let after_balloon_rss = after_balloon
        .total_rss_bytes
        .saturating_sub(baseline_helpers.total_rss_bytes);
    eprintln!();
    eprintln!(
        "==== After balloon to {} per VM (10s settle) ====",
        fmt_mb(target_per_vm)
    );
    eprintln!(
        "sum helper RSS:      {}",
        fmt_mb(after_balloon.total_rss_bytes)
    );
    eprintln!("delta vs baseline:   {}", fmt_mb(after_balloon_rss));
    let balloon_change_signed: i64 =
        after_balloon.total_rss_bytes as i64 - after_boot.total_rss_bytes as i64;
    eprintln!(
        "delta vs after-boot: {} MB  (negative = host RSS dropped; balloon worked)",
        balloon_change_signed / ONE_MB as i64
    );

    eprintln!();
    eprintln!("==== Stopping VMs ====");
    for vm in &vms {
        let _ = vm.stop().await;
    }
    drop(vms);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let after_stop = helper_processes();
    eprintln!("XPC helper count:    {}", after_stop.count);
    eprintln!(
        "sum helper RSS:      {}",
        fmt_mb(after_stop.total_rss_bytes)
    );
    eprintln!(
        "delta vs baseline:   {}",
        fmt_mb(
            after_stop
                .total_rss_bytes
                .saturating_sub(baseline_helpers.total_rss_bytes)
        )
    );
}
