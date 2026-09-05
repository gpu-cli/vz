//! Kernel-derived root/PID/mount observations, shared with offline regressions.

use anyhow::{Context, Result, ensure};

pub const EXEC_SCRIPT: &str = "set -eu; /bin/busybox stat -Lc '%d:%i' / /proc/self/root; /bin/busybox readlink /proc/self/ns/pid; /bin/busybox readlink /proc/self/ns/mnt; /bin/busybox readlink /proc/1/ns/pid; /bin/busybox readlink /proc/1/ns/mnt";

pub fn guest_script(pid: u64) -> Result<String> {
    ensure!(pid > 1 && pid <= i32::MAX as u64, "invalid init PID");
    Ok(format!(
        "set -eu; /bin/busybox stat -Lc '%d:%i' /proc/{pid}/root /; /bin/busybox readlink /proc/{pid}/ns/pid; /bin/busybox readlink /proc/{pid}/ns/mnt; /bin/busybox readlink /proc/1/ns/pid; /bin/busybox readlink /proc/1/ns/mnt"
    ))
}

fn number(value: &str, allow_zero: bool) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit())
            && (value == "0" || !value.starts_with('0')),
        "noncanonical kernel identity"
    );
    let parsed = value.parse::<u64>()?;
    ensure!(allow_zero || parsed != 0, "zero kernel identity");
    Ok(())
}

fn root(value: &str) -> Result<()> {
    let (device, inode) = value.split_once(':').context("missing root identity")?;
    number(device, true)?;
    number(inode, false)
}

fn namespace(value: &str, kind: &str) -> Result<()> {
    let inode = value
        .strip_prefix(&format!("{kind}:["))
        .and_then(|value| value.strip_suffix(']'))
        .context("incorrect namespace kind")?;
    number(inode, false)
}

/// Bracket the host exec with exact-init observations. Namespace equality alone
/// is insufficient: setns can restore a broader namespace root after chroot.
pub fn validate(before: &str, executed: &str, after: &str) -> Result<()> {
    ensure!(
        before.len() <= 1024 && executed.len() <= 1024 && after.len() <= 1024,
        "root observations exceed bounds"
    );
    ensure!(
        before == after,
        "container root/namespaces changed during exec"
    );
    let guest: Vec<_> = before.lines().collect();
    let exec: Vec<_> = executed.lines().collect();
    ensure!(
        guest.len() == 6 && exec.len() == 6,
        "incomplete root observations"
    );
    root(guest[0])?;
    root(guest[1])?;
    root(exec[0])?;
    root(exec[1])?;
    for (kind, target, host, actual, visible_init) in [
        ("pid", guest[2], guest[4], exec[2], exec[4]),
        ("mnt", guest[3], guest[5], exec[3], exec[5]),
    ] {
        for value in [target, host, actual, visible_init] {
            namespace(value, kind)?;
        }
        ensure!(target != host, "container shares guest {kind} namespace");
        ensure!(
            target == actual && actual == visible_init,
            "exec or visible proc init is outside container {kind} namespace"
        );
    }
    ensure!(guest[0] != guest[1], "container root equals guest root");
    ensure!(
        exec[0] == guest[0] && exec[1] == guest[0],
        "Docker exec sees a root other than its container init"
    );
    Ok(())
}
