//! `vz build` -- Build Dockerfiles into the local vz OCI store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Args;

use super::oci::ContainerOpts;

/// Build a Dockerfile using BuildKit and import the image into the local store.
#[derive(Args, Debug)]
pub struct BuildArgs {
    /// Build context directory.
    #[arg(default_value = ".")]
    pub context: PathBuf,

    /// Image name and optional tag (for example `myapp:latest`).
    #[arg(short = 't', long = "tag")]
    pub tag: Option<String>,

    /// Dockerfile path (relative to context unless absolute).
    #[arg(short = 'f', long = "file", default_value = "Dockerfile")]
    pub dockerfile: PathBuf,

    /// Multi-stage target to build.
    #[arg(long)]
    pub target: Option<String>,

    /// Build-time variable (`KEY=VALUE`). Can be repeated.
    #[arg(long = "build-arg", value_name = "KEY=VALUE")]
    pub build_args: Vec<String>,

    /// Disable BuildKit cache.
    #[arg(long)]
    pub no_cache: bool,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

/// Entry point for `vz build`.
pub async fn run(args: BuildArgs) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let config = super::oci::build_macos_runtime_config(&args.opts)?;
        let context_dir = expand_home_dir(&args.context);
        let tag = args.tag.unwrap_or_else(|| default_tag(&context_dir));
        let build_args = parse_build_args(&args.build_args)?;

        let request = vz_oci_macos::BuildRequest {
            context_dir,
            dockerfile: args.dockerfile,
            tag: tag.clone(),
            target: args.target,
            build_args,
            no_cache: args.no_cache,
        };

        let result = vz_oci_macos::buildkit::build_image(&config, request).await?;
        println!("Built {tag} as {}", result.image_id.0);
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = args;
        anyhow::bail!("`vz build` is currently supported only on macOS")
    }
}

fn parse_build_args(values: &[String]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for raw in values {
        let Some((key, value)) = raw.split_once('=') else {
            anyhow::bail!("invalid --build-arg `{raw}` (expected KEY=VALUE)");
        };
        if key.trim().is_empty() {
            anyhow::bail!("invalid --build-arg `{raw}` (empty key)");
        }
        parsed.insert(key.to_string(), value.to_string());
    }
    Ok(parsed)
}

fn default_tag(context: &Path) -> String {
    let stem = context
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("image");
    let mut out = String::new();
    for ch in stem.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    if out.trim_matches('-').is_empty() {
        "image:latest".to_string()
    } else {
        format!("{}:latest", out.trim_matches('-'))
    }
}

fn expand_home_dir(path: &Path) -> PathBuf {
    if let Some(path_str) = path.to_str() {
        if let Some(rest) = path_str.strip_prefix("~/")
            && let Some(home) = std::env::var_os("HOME")
        {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parse_build_args_supports_multiple_values() {
        let values = vec!["A=1".to_string(), "B=two".to_string()];
        let parsed = parse_build_args(&values).unwrap();
        assert_eq!(parsed.get("A").unwrap(), "1");
        assert_eq!(parsed.get("B").unwrap(), "two");
    }

    #[test]
    fn parse_build_args_rejects_missing_equals() {
        let values = vec!["BROKEN".to_string()];
        let err = parse_build_args(&values).unwrap_err();
        assert!(err.to_string().contains("expected KEY=VALUE"));
    }

    #[test]
    fn default_tag_uses_context_directory_name() {
        let tag = default_tag(Path::new("/tmp/My App"));
        assert_eq!(tag, "my-app:latest");
    }
}
