//! Installed native pins and private Machine disks. Catalog paths are trusted
//! operator inputs; a ProjectDefinition may select identities, never host paths.
use crate::machine_runtime_registry::MachineRuntimeStoreLease;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use vz_macos_provision::{
    artifact_cache::Artifact,
    bootstrap::{BootstrapCache, Progress, ReleaseManifest},
};
use vz_runtime_contract::{HostSpec, MachineSpec};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConfiguration {
    pub schema_version: u32,
    pub host: HostSpec,
    pub machine: MachineSpec,
    pub manifest: Artifact,
    pub cpus: u8,
    pub memory_mb: u64,
}

impl NativeConfiguration {
    pub fn digest(&self) -> Result<String> {
        let mut hash = Sha256::new();
        hash.update(b"vz.native-macos-configuration.v1\0");
        hash.update(serde_json::to_vec(self)?);
        Ok(format!("sha256:{hash:x}", hash = hash.finalize()))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    configuration: NativeConfiguration,
    release: ReleaseManifest,
}

pub struct NativePin {
    store: Arc<MachineRuntimeStoreLease>,
    configuration: NativeConfiguration,
    release: ReleaseManifest,
}

impl NativePin {
    pub fn store(&self) -> &Arc<MachineRuntimeStoreLease> {
        &self.store
    }
    pub fn configuration(&self) -> &NativeConfiguration {
        &self.configuration
    }
    pub fn release(&self) -> &ReleaseManifest {
        &self.release
    }
    pub fn directory(&self) -> PathBuf {
        self.store.data_path().join("native-target")
    }
    pub fn validate_current(&self) -> Result<()> {
        self.store.validate_current()?;
        let pin: Pin = serde_json::from_slice(&read_regular(
            &self.directory().join("pin.json"),
            128 * 1024,
        )?)?;
        ensure!(
            pin.configuration == self.configuration && pin.release == self.release,
            "native Machine pin changed"
        );
        let manifest_bytes = read_regular(&self.directory().join("manifest.json"), 64 * 1024)?;
        ensure!(
            manifest_bytes.len() as u64 == self.configuration.manifest.size_bytes
                && format!("{:x}", Sha256::digest(&manifest_bytes))
                    == self.configuration.manifest.sha256,
            "persisted native manifest no longer matches its authenticated pin"
        );
        let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes)?;
        ensure!(
            manifest == self.release,
            "native release differs from authenticated manifest"
        );
        for name in [
            "disk.img",
            "hardware-model",
            "auxiliary-storage",
            "machine-identifier",
        ] {
            let m = fs::symlink_metadata(self.directory().join(name))?;
            use std::os::unix::fs::MetadataExt;
            ensure!(
                m.is_file()
                    && m.nlink() == 1
                    && m.uid() == rustix::process::geteuid().as_raw()
                    && m.mode() & 0o077 == 0,
                "native Machine file is not private: {name}"
            );
        }
        ensure!(
            fs::metadata(self.directory().join("disk.img"))?.len()
                == self.release.prepared_image.size_bytes,
            "native disk size changed"
        );
        let hardware = read_regular(&self.directory().join("hardware-model"), 1024 * 1024)?;
        ensure!(
            format!("{:x}", Sha256::digest(&hardware))
                == self.release.platform.hardware_model.sha256,
            "native hardware model changed"
        );
        Ok(())
    }
}

pub fn read_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let fd = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    let file = File::from(fd);
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.len() <= limit,
        "invalid bounded native pin file"
    );
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "native pin exceeded limit");
    Ok(bytes)
}

pub fn load(
    store: Arc<MachineRuntimeStoreLease>,
    host: HostSpec,
    machine: &MachineSpec,
) -> Result<NativePin> {
    store.validate_current()?;
    let pin: Pin = serde_json::from_slice(&read_regular(
        &store.data_path().join("native-target/pin.json"),
        128 * 1024,
    )?)?;
    ensure!(
        pin.configuration.schema_version == 1
            && pin.configuration.host == host
            && &pin.configuration.machine == machine
            && pin.configuration.digest()? == store.configuration_digest(),
        "persisted native configuration does not match the Machine owner"
    );
    pin.configuration.manifest.validate()?;
    pin.release.validate()?;
    let native = NativePin {
        store,
        configuration: pin.configuration,
        release: pin.release,
    };
    native.validate_current()?;
    Ok(native)
}

pub async fn prepare(
    store: Arc<MachineRuntimeStoreLease>,
    configuration: NativeConfiguration,
    bundle: Option<&Path>,
    cache_root: PathBuf,
    progress: impl FnMut(Progress) -> Result<()>,
) -> Result<NativePin> {
    store.validate_current()?;
    ensure!(
        configuration.digest()? == store.configuration_digest(),
        "native configuration differs from reserved store"
    );
    let destination = store.data_path().join("native-target");
    if fs::symlink_metadata(&destination).is_ok() {
        return load(store, configuration.host, &configuration.machine);
    }
    let cache = BootstrapCache::new(cache_root.clone())?;
    let prepared = if let Some(bundle) = bundle {
        cache
            .prepare_installed(&configuration.manifest, bundle, progress)
            .await?
    } else {
        cache.prepare(&configuration.manifest, progress).await?
    };
    let release = prepared.manifest().clone();
    ensure!(
        configuration
            .machine
            .target
            .version
            .as_ref()
            .is_none_or(|v| v == &release.macos_version),
        "catalog version differs from bootstrap manifest"
    );
    ensure!(
        u32::from(configuration.cpus) >= release.platform.minimum_cpu_count
            && configuration.memory_mb * 1024 * 1024 >= release.platform.minimum_memory_bytes,
        "native resources below manifest minimum"
    );
    let host = std::process::Command::new("/usr/bin/sw_vers")
        .arg("-productVersion")
        .output()?;
    ensure!(host.status.success(), "cannot determine macOS host version");
    let numeric_version = |value: &str| -> Result<[u32; 3]> {
        let mut result = [0; 3];
        let parts: Vec<_> = value.trim().split('.').collect();
        ensure!((2..=3).contains(&parts.len()), "invalid OS version");
        for (slot, part) in result.iter_mut().zip(parts) {
            *slot = part.parse()?;
        }
        Ok(result)
    };
    ensure!(
        numeric_version(std::str::from_utf8(&host.stdout)?)?
            >= numeric_version(&release.platform.minimum_host_version)?,
        "macOS host is older than this release requires"
    );
    let manifest_bytes = read_regular(
        &cache_root
            .join("downloads")
            .join(&configuration.manifest.sha256),
        64 * 1024,
    )?;
    ensure!(
        manifest_bytes.len() as u64 == configuration.manifest.size_bytes
            && format!("{:x}", Sha256::digest(&manifest_bytes)) == configuration.manifest.sha256,
        "cached native manifest differs from pin"
    );
    use std::os::unix::fs::PermissionsExt;
    let staged = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(store.data_path())?;
    fs::write(staged.path().join("manifest.json"), manifest_bytes)?;
    prepared.clone_disk(&staged.path().join("disk.img"))?;
    fs::copy(
        prepared.hardware_model_path(),
        staged.path().join("hardware-model"),
    )?;
    fs::copy(
        prepared.auxiliary_storage_seed_path(),
        staged.path().join("auxiliary-storage"),
    )?;
    fs::write(
        staged.path().join("machine-identifier"),
        vz::install::generate_machine_id_data().map_err(|e| anyhow::anyhow!(e.to_string()))?,
    )?;
    fs::write(
        staged.path().join("pin.json"),
        serde_json::to_vec(&Pin {
            configuration: configuration.clone(),
            release,
        })?,
    )?;
    for name in [
        "disk.img",
        "hardware-model",
        "auxiliary-storage",
        "machine-identifier",
        "pin.json",
        "manifest.json",
    ] {
        fs::set_permissions(staged.path().join(name), fs::Permissions::from_mode(0o600))?;
        File::open(staged.path().join(name))?.sync_all()?;
    }
    store.validate_current()?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staged.path(),
        rustix::fs::CWD,
        &destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .context("publish private native Machine files")?;
    File::open(store.data_path())?.sync_all()?;
    load(store, configuration.host, &configuration.machine)
}
