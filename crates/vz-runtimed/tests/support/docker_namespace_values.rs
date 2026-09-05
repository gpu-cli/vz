//! Pure validation shared by the physical proof and a separate parser test target.

use anyhow::{Context, Result, ensure};

pub fn container_id(value: &str) -> Result<&str> {
    let value = value.trim();
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "expected one full lowercase hexadecimal Docker container ID"
    );
    Ok(value)
}

pub fn time_namespace(value: &str) -> Result<&str> {
    let value = value.trim();
    let inode = value
        .strip_prefix("time:[")
        .and_then(|value| value.strip_suffix(']'))
        .context("expected a Linux time namespace inode")?;
    ensure!(
        !inode.is_empty()
            && !inode.starts_with('0')
            && inode.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid time namespace inode"
    );
    ensure!(inode.parse::<u64>()? > 0, "zero time namespace inode");
    Ok(value)
}
