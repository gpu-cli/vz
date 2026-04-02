//! `vz init` — generate a `vz.json` project configuration file.
//!
//! Detects the project type from files in the current directory
//! (Cargo.toml, package.json, etc.) and generates an appropriate template.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use clap::Args;
use serde::Serialize;

const VZ_CONFIG_FILE: &str = "vz.json";

/// Initialize a vz.json configuration for the current project.
#[derive(Args, Debug)]
pub struct DevInitArgs {
    /// Force overwrite if vz.json already exists.
    #[arg(long)]
    pub force: bool,

    /// Project type override (rust, node, python, go, generic).
    #[arg(long, value_name = "TYPE")]
    pub template: Option<String>,

    /// Base container image override.
    #[arg(long)]
    pub image: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VzInitConfig {
    image: String,
    workspace: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mounts: Vec<VzInitMount>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    setup: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: BTreeMap<String, String>,
    resources: VzInitResources,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VzInitMount {
    source: String,
    target: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    read_only: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VzInitResources {
    cpus: u8,
    memory: String,
}

#[derive(Debug, Clone, Copy)]
enum ProjectType {
    Rust,
    Node,
    Python,
    Go,
    Generic,
}

impl ProjectType {
    fn detect(dir: &Path) -> Self {
        if dir.join("Cargo.toml").exists() {
            Self::Rust
        } else if dir.join("package.json").exists() {
            Self::Node
        } else if dir.join("pyproject.toml").exists()
            || dir.join("setup.py").exists()
            || dir.join("requirements.txt").exists()
        {
            Self::Python
        } else if dir.join("go.mod").exists() {
            Self::Go
        } else {
            Self::Generic
        }
    }

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "rust" => Ok(Self::Rust),
            "node" | "nodejs" | "javascript" | "typescript" => Ok(Self::Node),
            "python" | "py" => Ok(Self::Python),
            "go" | "golang" => Ok(Self::Go),
            "generic" | "default" => Ok(Self::Generic),
            other => bail!(
                "unknown template type '{other}'; expected one of: rust, node, python, go, generic"
            ),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Rust => "Rust",
            Self::Node => "Node.js",
            Self::Python => "Python",
            Self::Go => "Go",
            Self::Generic => "generic",
        }
    }
}

fn build_config(project_type: ProjectType, image_override: Option<&str>) -> VzInitConfig {
    let (image, setup, env, cpus, memory) = match project_type {
        ProjectType::Rust => (
            "ubuntu:24.04",
            vec![
                "apt-get update".to_string(),
                "apt-get install -y build-essential pkg-config libssl-dev curl".to_string(),
                "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y"
                    .to_string(),
            ],
            BTreeMap::from([("PATH".to_string(), "/root/.cargo/bin:$PATH".to_string())]),
            4,
            "8G",
        ),
        ProjectType::Node => (
            "ubuntu:24.04",
            vec![
                "apt-get update".to_string(),
                "apt-get install -y curl git".to_string(),
                "curl -fsSL https://deb.nodesource.com/setup_22.x | bash -".to_string(),
                "apt-get install -y nodejs".to_string(),
            ],
            BTreeMap::new(),
            2,
            "4G",
        ),
        ProjectType::Python => (
            "ubuntu:24.04",
            vec![
                "apt-get update".to_string(),
                "apt-get install -y python3 python3-pip python3-venv".to_string(),
            ],
            BTreeMap::new(),
            2,
            "4G",
        ),
        ProjectType::Go => (
            "ubuntu:24.04",
            vec![
                "apt-get update".to_string(),
                "apt-get install -y curl git".to_string(),
                "curl -fsSL https://go.dev/dl/go1.23.6.linux-arm64.tar.gz | tar -C /usr/local -xzf -"
                    .to_string(),
            ],
            BTreeMap::from([("PATH".to_string(), "/usr/local/go/bin:$PATH".to_string())]),
            2,
            "4G",
        ),
        ProjectType::Generic => (
            "ubuntu:24.04",
            vec![
                "apt-get update".to_string(),
                "apt-get install -y build-essential curl git".to_string(),
            ],
            BTreeMap::new(),
            2,
            "4G",
        ),
    };

    VzInitConfig {
        image: image_override.unwrap_or(image).to_string(),
        workspace: "/workspace".to_string(),
        mounts: vec![VzInitMount {
            source: ".".to_string(),
            target: "/workspace".to_string(),
            read_only: false,
        }],
        setup,
        env,
        resources: VzInitResources {
            cpus,
            memory: memory.to_string(),
        },
    }
}

pub async fn cmd_dev_init(args: DevInitArgs) -> anyhow::Result<()> {
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    let config_path = cwd.join(VZ_CONFIG_FILE);

    if config_path.exists() && !args.force {
        bail!(
            "{VZ_CONFIG_FILE} already exists. Use --force to overwrite."
        );
    }

    let project_type = match &args.template {
        Some(t) => ProjectType::from_str(t)?,
        None => ProjectType::detect(&cwd),
    };

    let config = build_config(project_type, args.image.as_deref());
    let json = serde_json::to_string_pretty(&config).context("failed to serialize config")?;

    std::fs::write(&config_path, format!("{json}\n"))
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let dir_name = cwd
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("project");

    eprintln!(
        "Created {VZ_CONFIG_FILE} for {} project '{dir_name}'",
        project_type.label()
    );
    eprintln!("\nNext steps:");
    eprintln!("  vz run <command>    Run a command in the Linux VM");
    eprintln!("  vz stop             Stop the VM when done");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rust_config_has_cargo() {
        let config = build_config(ProjectType::Rust, None);
        assert!(config.setup.iter().any(|s| s.contains("rustup")));
        assert!(config.env.contains_key("PATH"));
        assert_eq!(config.resources.cpus, 4);
    }

    #[test]
    fn build_node_config_has_nodejs() {
        let config = build_config(ProjectType::Node, None);
        assert!(config.setup.iter().any(|s| s.contains("nodejs")));
    }

    #[test]
    fn image_override_works() {
        let config = build_config(ProjectType::Generic, Some("debian:12"));
        assert_eq!(config.image, "debian:12");
    }

    #[test]
    fn project_type_from_str_works() {
        assert!(matches!(
            ProjectType::from_str("rust").unwrap(),
            ProjectType::Rust
        ));
        assert!(matches!(
            ProjectType::from_str("node").unwrap(),
            ProjectType::Node
        ));
        assert!(ProjectType::from_str("invalid").is_err());
    }

    #[test]
    fn default_mount_is_workspace() {
        let config = build_config(ProjectType::Generic, None);
        assert_eq!(config.mounts.len(), 1);
        assert_eq!(config.mounts[0].source, ".");
        assert_eq!(config.mounts[0].target, "/workspace");
    }
}
