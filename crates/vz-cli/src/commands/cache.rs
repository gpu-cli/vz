//! `vz cache` -- Manage cached files.

use std::path::Path;

use clap::{Args, Subcommand};
use tracing::info;

use crate::registry;

/// Manage cached files (IPSWs, downloads).
#[derive(Args, Debug)]
pub struct CacheArgs {
    #[command(subcommand)]
    pub action: CacheAction,
}

#[derive(Subcommand, Debug)]
pub enum CacheAction {
    /// Show cached files and their sizes.
    List,
    /// Delete cached files.
    Clean {
        /// Delete everything (including images and states).
        #[arg(long)]
        all: bool,
    },
}

pub async fn run(args: CacheArgs) -> anyhow::Result<()> {
    let vz_home = registry::vz_home();

    match args.action {
        CacheAction::List => {
            list_cache(&vz_home)?;
        }
        CacheAction::Clean { all } => {
            clean_cache(&vz_home, all)?;
        }
    }

    Ok(())
}

fn list_cache(vz_home: &Path) -> anyhow::Result<()> {
    let dirs = [
        ("cache", vz_home.join("cache")),
        ("images", vz_home.join("images")),
        ("states", vz_home.join("states")),
    ];

    for (label, dir) in &dirs {
        if dir.exists() {
            let size = dir_size(dir)?;
            let file_count = count_files(dir)?;
            info!(
                dir = %dir.display(),
                files = file_count,
                size_mb = size / (1024 * 1024),
                "{label}"
            );
            println!(
                "{:<10} {:>5} files  {:>8}",
                label,
                file_count,
                format_size(size)
            );
        } else {
            println!("{label:<10}     0 files         0B");
        }
    }

    Ok(())
}

fn clean_cache(vz_home: &Path, all: bool) -> anyhow::Result<()> {
    let cache_dir = vz_home.join("cache");

    if cache_dir.exists() {
        let size = dir_size(&cache_dir)?;
        std::fs::remove_dir_all(&cache_dir)?;
        std::fs::create_dir_all(&cache_dir)?;
        info!(freed = format_size(size), "cleaned cache directory");
        println!("Cleaned cache: freed {}", format_size(size));
    }

    if all {
        for dir_name in &["images", "states"] {
            let dir = vz_home.join(dir_name);
            if dir.exists() {
                let size = dir_size(&dir)?;
                std::fs::remove_dir_all(&dir)?;
                std::fs::create_dir_all(&dir)?;
                info!(freed = format_size(size), "cleaned {dir_name} directory");
                println!("Cleaned {dir_name}: freed {}", format_size(size));
            }
        }
    }

    Ok(())
}

fn dir_size(path: &Path) -> anyhow::Result<u64> {
    let mut total = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total += metadata.len();
            } else if metadata.is_dir() {
                total += dir_size(&entry.path())?;
            }
        }
    }
    Ok(total)
}

fn count_files(path: &Path) -> anyhow::Result<u64> {
    let mut count = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                count += 1;
            } else if metadata.is_dir() {
                count += count_files(&entry.path())?;
            }
        }
    }
    Ok(count)
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}K", bytes as f64 / KB as f64)
    } else {
        format!("{bytes}B")
    }
}
