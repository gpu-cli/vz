use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SPACE_CACHE_KEY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceCacheRuntimeIdentity {
    pub base_image_ref: Option<String>,
    pub main_container: Option<String>,
    pub cpus: u8,
    pub memory_mb: u64,
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceCacheKeyMaterial {
    pub cache_name: String,
    pub project_root: String,
    pub config_path: String,
    pub input_hashes: BTreeMap<String, String>,
    pub runtime: SpaceCacheRuntimeIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceCacheKey {
    pub schema_version: u16,
    pub cache_name: String,
    pub digest_hex: String,
    pub canonical_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SpaceCacheCanonicalKeyV1 {
    schema_version: u16,
    cache_name: String,
    project_root: String,
    config_path: String,
    input_hashes: BTreeMap<String, String>,
    runtime: SpaceCacheRuntimeIdentity,
}

impl SpaceCacheKey {
    pub fn from_material(material: SpaceCacheKeyMaterial) -> anyhow::Result<Self> {
        Self::from_material_with_version(material, SPACE_CACHE_KEY_SCHEMA_VERSION)
    }

    pub fn from_material_with_version(
        material: SpaceCacheKeyMaterial,
        schema_version: u16,
    ) -> anyhow::Result<Self> {
        if material.cache_name.trim().is_empty() {
            bail!("cache_name cannot be empty");
        }
        if material.project_root.trim().is_empty() {
            bail!("project_root cannot be empty");
        }
        if material.config_path.trim().is_empty() {
            bail!("config_path cannot be empty");
        }
        if material.input_hashes.is_empty() {
            bail!("input_hashes cannot be empty");
        }

        let canonical = SpaceCacheCanonicalKeyV1 {
            schema_version,
            cache_name: material.cache_name.trim().to_string(),
            project_root: material.project_root.trim().to_string(),
            config_path: material.config_path.trim().to_string(),
            input_hashes: material.input_hashes,
            runtime: material.runtime,
        };
        let canonical_json = serde_json::to_string(&canonical)
            .context("failed to serialize canonical space cache key")?;
        let mut hasher = Sha256::new();
        hasher.update(canonical_json.as_bytes());
        let digest_hex = format!("{:x}", hasher.finalize());
        Ok(Self {
            schema_version,
            cache_name: canonical.cache_name,
            digest_hex,
            canonical_json,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpaceCacheLookup {
    Hit,
    MissNotFound,
    MissKeyMismatch,
    MissVersionMismatch { requested: u16, stored: u16 },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceCacheIndex {
    entries: BTreeMap<String, SpaceCacheKey>,
}

impl SpaceCacheIndex {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.is_file() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read space cache index {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse space cache index {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create parent directory for space cache index {}",
                    path.display()
                )
            })?;
        }
        let json = serde_json::to_vec_pretty(self).context("failed to serialize cache index")?;
        std::fs::write(path, json)
            .with_context(|| format!("failed to write space cache index {}", path.display()))?;
        Ok(())
    }

    pub fn lookup(&self, key: &SpaceCacheKey) -> SpaceCacheLookup {
        let Some(stored) = self.entries.get(&key.cache_name) else {
            return SpaceCacheLookup::MissNotFound;
        };
        if stored.schema_version != key.schema_version {
            return SpaceCacheLookup::MissVersionMismatch {
                requested: key.schema_version,
                stored: stored.schema_version,
            };
        }
        if stored.canonical_json == key.canonical_json {
            SpaceCacheLookup::Hit
        } else {
            SpaceCacheLookup::MissKeyMismatch
        }
    }

    pub fn upsert(&mut self, key: SpaceCacheKey) {
        self.entries.insert(key.cache_name.clone(), key);
    }

    pub fn invalidate_for_schema(&mut self, supported_schema_version: u16) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, key| key.schema_version == supported_schema_version);
        before.saturating_sub(self.entries.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture_material() -> SpaceCacheKeyMaterial {
        SpaceCacheKeyMaterial {
            cache_name: "deps".to_string(),
            project_root: "/workspace/project".to_string(),
            config_path: "/workspace/project/vz.json".to_string(),
            input_hashes: BTreeMap::from([("package-lock.json".to_string(), "a".repeat(64))]),
            runtime: SpaceCacheRuntimeIdentity {
                base_image_ref: Some("ubuntu:24.04".to_string()),
                main_container: Some("workspace-main".to_string()),
                cpus: 4,
                memory_mb: 8192,
                os: "linux".to_string(),
                arch: "x86_64".to_string(),
            },
        }
    }

    #[test]
    fn canonical_cache_key_serialization_is_deterministic_and_versioned() {
        let key1 = SpaceCacheKey::from_material(fixture_material()).expect("key");
        let key2 = SpaceCacheKey::from_material(fixture_material()).expect("key");
        assert_eq!(key1.schema_version, SPACE_CACHE_KEY_SCHEMA_VERSION);
        assert_eq!(key1.canonical_json, key2.canonical_json);
        assert_eq!(key1.digest_hex, key2.digest_hex);
    }

    #[test]
    fn identical_dimensions_hit_cache() {
        let dir = tempdir().expect("tempdir");
        let index_path = dir.path().join("space-cache-index.json");
        let key = SpaceCacheKey::from_material(fixture_material()).expect("key");

        let mut index = SpaceCacheIndex::default();
        index.upsert(key.clone());
        index.save(&index_path).expect("save index");

        let loaded = SpaceCacheIndex::load(&index_path).expect("load index");
        assert_eq!(loaded.lookup(&key), SpaceCacheLookup::Hit);
    }

    #[test]
    fn changed_dimension_misses_cache() {
        let mut index = SpaceCacheIndex::default();
        let key = SpaceCacheKey::from_material(fixture_material()).expect("key");
        index.upsert(key);

        let mut changed = fixture_material();
        changed.runtime.memory_mb = 16384;
        let changed_key = SpaceCacheKey::from_material(changed).expect("changed key");
        assert_eq!(
            index.lookup(&changed_key),
            SpaceCacheLookup::MissKeyMismatch
        );
    }

    #[test]
    fn schema_version_mismatch_is_explicit_and_invalidation_is_deterministic() {
        let mut index = SpaceCacheIndex::default();
        let key_v1 =
            SpaceCacheKey::from_material_with_version(fixture_material(), 1).expect("key_v1");
        let key_v2 =
            SpaceCacheKey::from_material_with_version(fixture_material(), 2).expect("key_v2");
        index.upsert(key_v2.clone());

        assert_eq!(
            index.lookup(&key_v1),
            SpaceCacheLookup::MissVersionMismatch {
                requested: 1,
                stored: 2
            }
        );
        assert_eq!(index.invalidate_for_schema(1), 1);
        assert_eq!(index.lookup(&key_v1), SpaceCacheLookup::MissNotFound);
    }
}
