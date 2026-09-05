//! Strict evidence checks; an exit code alone is never a device-policy proof.

use anyhow::{Context, Result, ensure};
use serde_json::Value;

pub const DENIED_OPEN: &str =
    "dd: can't open '/dev/vz-policy-loop-control': Operation not permitted\n";

pub fn capability_status(stdout: &str) -> Result<(u64, u64)> {
    let mut capabilities = None;
    let mut seccomp = None;
    for line in stdout.lines() {
        if let Some(value) = line.strip_prefix("CapEff:\t") {
            ensure!(capabilities.is_none(), "duplicate CapEff observation");
            ensure!(value.len() == 16, "invalid CapEff width");
            capabilities = Some(u64::from_str_radix(value, 16)?);
        } else if let Some(value) = line.strip_prefix("Seccomp:\t") {
            ensure!(seccomp.is_none(), "duplicate Seccomp observation");
            seccomp = Some(value.parse::<u64>()?);
        } else {
            anyhow::bail!("unexpected capability observation: {line}");
        }
    }
    let capabilities = capabilities.context("missing CapEff")?;
    let seccomp = seccomp.context("missing Seccomp")?;
    ensure!(capabilities & (1 << 27) != 0, "CAP_MKNOD is not effective");
    ensure!(seccomp == 2, "default seccomp filter is not active");
    Ok((capabilities, seccomp))
}

pub fn denied_open(command: &Value) -> Result<()> {
    ensure!(
        command["exit_code"] == 1,
        "unexpected denied-open exit status"
    );
    ensure!(command["stdout"] == "", "unexpected denied-open stdout");
    ensure!(
        command["stderr"] == DENIED_OPEN,
        "open did not report exact C-locale EPERM"
    );
    Ok(())
}
