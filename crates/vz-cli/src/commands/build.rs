//! `vz build` -- Build Dockerfiles into the local vz OCI store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand, ValueEnum};

use super::oci::ContainerOpts;

/// Build a Dockerfile or manage BuildKit cache.
#[derive(Args, Debug)]
pub struct BuildArgs {
    /// BuildKit-related subcommands.
    #[command(subcommand)]
    pub subcommand: Option<BuildSubcommand>,

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

    /// Build secret forwarded to BuildKit (`id=...,src=...`). Can be repeated.
    #[arg(long = "secret", value_name = "SPEC")]
    pub secrets: Vec<String>,

    /// Disable BuildKit cache.
    #[arg(long)]
    pub no_cache: bool,

    /// Push image to registry after build.
    #[arg(long)]
    pub push: bool,

    /// Explicit output specification (currently supports `type=oci,dest=<path>`).
    #[arg(short = 'o', long = "output")]
    pub output: Option<String>,

    /// Progress output mode.
    #[arg(long, value_enum, default_value_t = ProgressArg::Auto)]
    pub progress: ProgressArg,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Subcommand, Debug)]
pub enum BuildSubcommand {
    /// Manage BuildKit cache.
    Cache(BuildCacheArgs),
}

#[derive(Args, Debug)]
pub struct BuildCacheArgs {
    #[command(subcommand)]
    pub action: BuildCacheAction,
}

#[derive(Subcommand, Debug)]
pub enum BuildCacheAction {
    /// Show cache usage details.
    Du,
    /// Prune cache entries.
    Prune(BuildCachePruneArgs),
}

#[derive(Args, Debug)]
pub struct BuildCachePruneArgs {
    /// Remove all cache entries.
    #[arg(long)]
    pub all: bool,

    /// Keep cache newer than this duration (for example `24h`).
    #[arg(long = "keep-duration")]
    pub keep_duration: Option<String>,

    /// Keep this amount of storage (for example `5GB`).
    #[arg(long = "keep-storage")]
    pub keep_storage: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ProgressArg {
    #[default]
    Auto,
    Plain,
    Tty,
}

impl From<ProgressArg> for vz_oci_macos::buildkit::BuildProgress {
    fn from(value: ProgressArg) -> Self {
        match value {
            ProgressArg::Auto => Self::Auto,
            ProgressArg::Plain => Self::Plain,
            ProgressArg::Tty => Self::Tty,
        }
    }
}

/// Entry point for `vz build`.
pub async fn run(args: BuildArgs) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let config = super::oci::build_macos_runtime_config(&args.opts)?;

        if let Some(subcommand) = args.subcommand {
            return run_subcommand(config, subcommand).await;
        }

        let context_dir = expand_home_dir(&args.context);
        let tag = args.tag.unwrap_or_else(|| default_tag(&context_dir));
        let build_args = parse_build_args(&args.build_args)?;
        let secrets = parse_secrets(&args.secrets)?;
        let output = parse_output_mode(args.push, args.output.as_deref())?;

        let request = vz_oci_macos::BuildRequest {
            context_dir,
            dockerfile: args.dockerfile,
            tag,
            target: args.target,
            build_args,
            secrets,
            no_cache: args.no_cache,
            output,
            progress: args.progress.into(),
        };

        let result = vz_oci_macos::buildkit::build_image(&config, request).await?;
        match (&result.image_id, &result.output_path, result.pushed) {
            (Some(image_id), _, _) => println!("Built {} as {}", result.tag, image_id.0),
            (_, Some(path), _) => {
                println!(
                    "Built {} and wrote OCI archive to {}",
                    result.tag,
                    path.display()
                )
            }
            (_, _, true) => println!("Built and pushed {}", result.tag),
            _ => println!("Built {}", result.tag),
        }

        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = args;
        anyhow::bail!("`vz build` is currently supported only on macOS")
    }
}

#[cfg(target_os = "macos")]
async fn run_subcommand(
    config: vz_oci_macos::RuntimeConfig,
    subcommand: BuildSubcommand,
) -> anyhow::Result<()> {
    match subcommand {
        BuildSubcommand::Cache(cache) => match cache.action {
            BuildCacheAction::Du => {
                let output = vz_oci_macos::buildkit::cache_disk_usage(&config).await?;
                if output.trim().is_empty() {
                    println!("No BuildKit cache entries");
                } else {
                    println!("{output}");
                }
            }
            BuildCacheAction::Prune(prune) => {
                let output = vz_oci_macos::buildkit::cache_prune(
                    &config,
                    vz_oci_macos::buildkit::CachePruneOptions {
                        all: prune.all,
                        keep_duration: prune.keep_duration,
                        keep_storage: prune.keep_storage,
                    },
                )
                .await?;
                if output.trim().is_empty() {
                    println!("BuildKit cache prune complete");
                } else {
                    println!("{output}");
                }
            }
        },
    }
    Ok(())
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

fn parse_output_mode(
    push: bool,
    output_spec: Option<&str>,
) -> anyhow::Result<vz_oci_macos::buildkit::BuildOutput> {
    if push && output_spec.is_some() {
        anyhow::bail!("--push cannot be combined with --output");
    }

    if push {
        return Ok(vz_oci_macos::buildkit::BuildOutput::RegistryPush);
    }

    let Some(output_spec) = output_spec else {
        return Ok(vz_oci_macos::buildkit::BuildOutput::VzStore);
    };

    let mut fields = BTreeMap::new();
    for chunk in output_spec.split(',') {
        let Some((key, value)) = chunk.split_once('=') else {
            anyhow::bail!("invalid --output field `{chunk}` (expected key=value)");
        };
        fields.insert(key.trim().to_string(), value.trim().to_string());
    }

    let output_type = fields
        .remove("type")
        .ok_or_else(|| anyhow::anyhow!("--output requires `type=` field"))?;
    if output_type != "oci" {
        anyhow::bail!("unsupported --output type `{output_type}` (only `type=oci` is supported)");
    }

    let dest = fields
        .remove("dest")
        .ok_or_else(|| anyhow::anyhow!("--output requires `dest=` field for `type=oci`"))?;

    if !fields.is_empty() {
        let extras = fields.keys().cloned().collect::<Vec<_>>().join(", ");
        anyhow::bail!("unsupported --output field(s): {extras}");
    }

    Ok(vz_oci_macos::buildkit::BuildOutput::OciTar {
        dest: expand_home_dir(Path::new(&dest)),
    })
}

fn parse_secrets(values: &[String]) -> anyhow::Result<Vec<String>> {
    let mut parsed = Vec::with_capacity(values.len());
    for raw in values {
        if !raw.contains('=') {
            anyhow::bail!(
                "invalid --secret `{raw}` (expected key=value pairs like id=...,src=...)"
            );
        }
        parsed.push(raw.clone());
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
    fn parse_secrets_accepts_specs() {
        let values = vec![
            "id=npmrc,src=.npmrc".to_string(),
            "id=token,env=NPM_TOKEN".to_string(),
        ];
        let parsed = parse_secrets(&values).unwrap();
        assert_eq!(parsed, values);
    }

    #[test]
    fn parse_secrets_rejects_missing_equals() {
        let values = vec!["npmrc".to_string()];
        let err = parse_secrets(&values).unwrap_err();
        assert!(err.to_string().contains("expected key=value"));
    }

    #[test]
    fn parse_output_mode_defaults_to_vz_store() {
        let parsed = parse_output_mode(false, None).unwrap();
        assert!(matches!(
            parsed,
            vz_oci_macos::buildkit::BuildOutput::VzStore
        ));
    }

    #[test]
    fn parse_output_mode_supports_push() {
        let parsed = parse_output_mode(true, None).unwrap();
        assert!(matches!(
            parsed,
            vz_oci_macos::buildkit::BuildOutput::RegistryPush
        ));
    }

    #[test]
    fn parse_output_mode_supports_oci_tar() {
        let parsed = parse_output_mode(false, Some("type=oci,dest=./image.tar")).unwrap();
        match parsed {
            vz_oci_macos::buildkit::BuildOutput::OciTar { dest } => {
                assert_eq!(dest, PathBuf::from("./image.tar"));
            }
            _ => panic!("unexpected output mode"),
        }
    }

    #[test]
    fn parse_output_mode_rejects_push_and_output() {
        let err = parse_output_mode(true, Some("type=oci,dest=./image.tar")).unwrap_err();
        assert!(err.to_string().contains("cannot be combined"));
    }

    #[test]
    fn default_tag_uses_context_directory_name() {
        let tag = default_tag(Path::new("/tmp/My App"));
        assert_eq!(tag, "my-app:latest");
    }
}
