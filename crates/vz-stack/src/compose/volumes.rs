use super::helpers::val;
use super::validation::validate_volume_keys;
use super::*;

pub(super) fn parse_volumes(root: &serde_yml::Mapping) -> Result<Vec<VolumeSpec>, StackError> {
    let Some(value) = root.get(val("volumes")) else {
        return Ok(vec![]);
    };

    let volumes_map = value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("top-level `volumes` must be a mapping".into()))?;

    let mut volumes = Vec::new();
    for (key, vol_value) in volumes_map {
        let vol_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("volume name must be a string".into()))?;

        // Empty value (just the name) is valid — uses defaults.
        if vol_value.is_null() {
            volumes.push(VolumeSpec {
                name: vol_name.to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            });
            continue;
        }

        let vol_map = vol_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!("volume `{vol_name}` must be a mapping or empty"))
        })?;

        validate_volume_keys(vol_name, vol_map)?;

        let driver = vol_map
            .get(val("driver"))
            .and_then(|v| v.as_str())
            .unwrap_or("local")
            .to_string();

        if driver != "local" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("volumes.{vol_name}.driver"),
                reason: format!("only `local` driver is supported; got `{driver}`"),
            });
        }

        let driver_opts = vol_map
            .get(val("driver_opts"))
            .and_then(|v| v.as_mapping())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
                    .collect::<HashMap<String, String>>()
            });

        volumes.push(VolumeSpec {
            name: vol_name.to_string(),
            driver,
            driver_opts,
        });
    }

    // Sort for determinism.
    volumes.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(volumes)
}
