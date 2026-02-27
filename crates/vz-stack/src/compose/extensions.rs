use super::helpers::val;
use super::*;

pub(super) fn parse_xvz_disk_size(root: &serde_yml::Mapping) -> Result<Option<u64>, StackError> {
    let Some(xvz_value) = root.get(val("x-vz")) else {
        return Ok(None);
    };
    let xvz_map = xvz_value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("`x-vz` must be a mapping".into()))?;

    let Some(size_value) = xvz_map.get(val("disk_size")) else {
        return Ok(None);
    };

    // Accept integer (megabytes) or string with unit suffix.
    if let Some(n) = size_value.as_u64() {
        return Ok(Some(n));
    }
    if let Some(s) = size_value.as_str() {
        let s = s.trim().to_lowercase();
        return parse_size_to_mb(&s).map(Some).ok_or_else(|| {
            StackError::ComposeParse(format!(
                "x-vz.disk_size: invalid size `{s}`; use e.g. `10g`, `512m`, `1024`"
            ))
        });
    }

    Err(StackError::ComposeParse(
        "x-vz.disk_size must be a number (MB) or string with unit (e.g., `10g`, `512m`)".into(),
    ))
}

/// Parse a human-readable size string to megabytes.
///
/// Accepts: `"10g"`, `"10gb"`, `"512m"`, `"512mb"`, `"1024"` (plain = MB).
pub(super) fn parse_size_to_mb(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Strip unit suffix and compute multiplier.
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix('g') {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n.trim(), 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n.trim(), 1u64)
    } else if let Some(n) = s.strip_suffix("kb") {
        let val: u64 = n.trim().parse().ok()?;
        return Some(val.div_ceil(1024));
    } else if let Some(n) = s.strip_suffix('k') {
        let val: u64 = n.trim().parse().ok()?;
        return Some(val.div_ceil(1024));
    } else {
        // Plain number = megabytes.
        (s, 1u64)
    };

    let val: u64 = num_str.parse().ok()?;
    Some(val * multiplier)
}
