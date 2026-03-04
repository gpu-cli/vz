//! macOS IPSW installation.
//!
//! Creates a bootable macOS VM disk image from an Apple IPSW restore image.
//! This is a one-time operation for creating golden images.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use objc2::AnyThread;
use objc2::rc::Retained;
use objc2_foundation::NSData;
use objc2_virtualization::{
    VZMacAuxiliaryStorage, VZMacAuxiliaryStorageInitializationOptions,
    VZMacGraphicsDeviceConfiguration, VZMacGraphicsDisplayConfiguration, VZMacHardwareModel,
    VZMacMachineIdentifier, VZMacOSBootLoader, VZMacOSInstaller, VZMacOSRestoreImage,
    VZMacPlatformConfiguration, VZNATNetworkDeviceAttachment,
    VZUSBScreenCoordinatePointingDeviceConfiguration, VZVirtioBlockDeviceConfiguration,
    VZVirtioNetworkDeviceConfiguration, VZVirtualMachine, VZVirtualMachineConfiguration,
};
use tokio::sync::oneshot;
use tracing::{debug, info};

use crate::bridge::{SerialQueue, VMDelegate, completion_handler_block, nsurl_from_path};
use crate::error::VzError;
use crate::vm::VmState;

/// Source for macOS IPSW restore images.
pub enum IpswSource {
    /// Download the latest supported IPSW from Apple.
    Latest,
    /// Use a local IPSW file.
    Path(PathBuf),
}

/// Result of a macOS installation, containing the platform identity files.
///
/// These paths must be preserved and passed to `VmConfigBuilder::mac_platform()`
/// when booting the installed VM.
#[derive(Debug, Clone)]
pub struct InstallResult {
    /// Path to the disk image.
    pub disk_path: PathBuf,
    /// Path to the persisted hardware model data.
    pub hardware_model_path: PathBuf,
    /// Path to the persisted machine identifier data.
    pub machine_identifier_path: PathBuf,
    /// Path to the auxiliary storage (NVRAM equivalent).
    pub auxiliary_storage_path: PathBuf,
}

/// Install macOS from an IPSW into a disk image.
///
/// This is a long-running operation (10-30 minutes). It:
/// 1. Loads or fetches the restore image
/// 2. Creates a sparse disk image at `disk_path`
/// 3. Generates platform identity (hardware model, machine ID, aux storage)
/// 4. Runs `VZMacOSInstaller` to install macOS
/// 5. Persists platform files alongside the disk image
///
/// After installation, create a `VmConfig` with the returned paths
/// and call `Vm::create()` + `vm.start()` to boot.
pub async fn install_macos(
    ipsw: IpswSource,
    disk_path: &Path,
    disk_size_bytes: u64,
) -> Result<InstallResult, VzError> {
    // Step 1: Load the restore image and extract configuration requirements.
    // The restore image callback returns ObjC objects that are not Send,
    // so we extract the raw data we need (hardware model bytes, min CPU/memory)
    // and drop the ObjC objects before crossing any thread boundary.
    info!("loading restore image...");
    let requirements = load_restore_requirements(&ipsw).await?;

    info!(
        min_cpus = requirements.min_cpus,
        min_memory_bytes = requirements.min_memory,
        "extracted configuration requirements"
    );

    // Step 2: Create the sparse disk image
    info!(disk_path = %disk_path.display(), size_bytes = disk_size_bytes, "creating sparse disk image");
    create_sparse_disk_image(disk_path, disk_size_bytes)?;

    // Step 3: Persist hardware model data
    let hw_model_path = disk_path.with_extension("hwmodel");
    std::fs::write(&hw_model_path, &requirements.hw_model_data).map_err(|e| {
        VzError::InstallFailed(format!(
            "failed to write hardware model to {}: {e}",
            hw_model_path.display()
        ))
    })?;
    debug!(path = %hw_model_path.display(), "persisted hardware model");

    // Step 4: Generate and persist machine identifier
    let machine_id_path = disk_path.with_extension("machineid");
    let machine_id_data = generate_machine_id_data()?;
    std::fs::write(&machine_id_path, &machine_id_data).map_err(|e| {
        VzError::InstallFailed(format!(
            "failed to write machine identifier to {}: {e}",
            machine_id_path.display()
        ))
    })?;
    debug!(path = %machine_id_path.display(), "persisted machine identifier");

    // Step 5: Determine the IPSW file path for the installer
    let ipsw_path = resolve_ipsw_path(&ipsw)?;

    // Step 6: Run the installation on the dispatch queue
    let aux_path = disk_path.with_extension("aux");

    // Use at least the minimum required CPUs/memory, but prefer reasonable defaults
    let cpus = std::cmp::max(requirements.min_cpus, 4);
    let memory = std::cmp::max(requirements.min_memory, 8 * 1024 * 1024 * 1024);

    info!("building installation VM and starting macOS install...");
    run_installation(
        &requirements.hw_model_data,
        &machine_id_data,
        &aux_path,
        disk_path,
        &ipsw_path,
        cpus,
        memory,
    )
    .await?;

    info!("macOS installation completed successfully");

    Ok(InstallResult {
        disk_path: disk_path.to_path_buf(),
        hardware_model_path: hw_model_path,
        machine_identifier_path: machine_id_path,
        auxiliary_storage_path: aux_path,
    })
}

// ---------------------------------------------------------------------------
// Restore image loading
// ---------------------------------------------------------------------------

/// Extracted requirements from a restore image (all Send-safe data).
struct RestoreRequirements {
    /// Serialized hardware model data (from VZMacHardwareModel.dataRepresentation).
    hw_model_data: Vec<u8>,
    /// Minimum required CPU count.
    min_cpus: usize,
    /// Minimum required memory in bytes.
    min_memory: u64,
}

/// Load a restore image and extract requirements as plain Rust data.
///
/// ObjC objects from the restore image are not Send, so we extract everything
/// we need as byte vectors and integers, then drop the ObjC references.
async fn load_restore_requirements(source: &IpswSource) -> Result<RestoreRequirements, VzError> {
    let (tx, rx) = oneshot::channel();
    let tx = Cell::new(Some(tx));

    let block = block2::RcBlock::new(
        move |image: *mut VZMacOSRestoreImage, error: *mut objc2_foundation::NSError| {
            let result = if !error.is_null() {
                let err = unsafe { &*error };
                Err(VzError::InstallFailed(format!(
                    "failed to load restore image: {}",
                    err.localizedDescription()
                )))
            } else if image.is_null() {
                Err(VzError::InstallFailed(
                    "restore image load returned null".into(),
                ))
            } else {
                // SAFETY: Non-null pointer from the framework callback.
                let image_ref = unsafe { &*image };

                // Extract configuration requirements
                let config_req = unsafe { image_ref.mostFeaturefulSupportedConfiguration() }
                    .ok_or_else(|| {
                        VzError::InstallFailed(
                            "this restore image is not supported on the current host hardware"
                                .into(),
                        )
                    });

                match config_req {
                    Ok(req) => {
                        let hw_model = unsafe { req.hardwareModel() };
                        if !unsafe { hw_model.isSupported() } {
                            Err(VzError::InstallFailed(
                                "the hardware model from this restore image is not supported \
                                 on this host"
                                    .into(),
                            ))
                        } else {
                            let hw_data = unsafe { hw_model.dataRepresentation() };
                            Ok(RestoreRequirements {
                                hw_model_data: hw_data.to_vec(),
                                min_cpus: unsafe { req.minimumSupportedCPUCount() },
                                min_memory: unsafe { req.minimumSupportedMemorySize() },
                            })
                        }
                    }
                    Err(e) => Err(e),
                }
            };
            if let Some(tx) = tx.take() {
                let _ = tx.send(result);
            }
        },
    );

    match source {
        IpswSource::Path(path) => {
            let url = nsurl_from_path(path);
            // SAFETY: loadFileURL_completionHandler loads a restore image from
            // a local file. The completion handler is called on an arbitrary thread.
            unsafe {
                VZMacOSRestoreImage::loadFileURL_completionHandler(&url, &block);
            }
        }
        IpswSource::Latest => {
            // SAFETY: fetchLatestSupportedWithCompletionHandler fetches metadata
            // about the latest supported restore image from Apple's servers.
            unsafe {
                VZMacOSRestoreImage::fetchLatestSupportedWithCompletionHandler(&block);
            }
        }
    }

    rx.await
        .map_err(|_| VzError::InstallFailed("restore image callback was never invoked".into()))?
}

/// Fetch the download URL for the latest supported macOS restore image from Apple.
///
/// Contacts Apple's servers to determine the latest compatible restore image
/// for this host's hardware. Returns the HTTPS URL for downloading the IPSW.
pub async fn fetch_latest_ipsw_url() -> Result<String, VzError> {
    let (tx, rx) = oneshot::channel();
    let tx = Cell::new(Some(tx));

    let block = block2::RcBlock::new(
        move |image: *mut VZMacOSRestoreImage, error: *mut objc2_foundation::NSError| {
            let result = if !error.is_null() {
                let err = unsafe { &*error };
                Err(VzError::InstallFailed(format!(
                    "failed to fetch restore image info: {}",
                    err.localizedDescription()
                )))
            } else if image.is_null() {
                Err(VzError::InstallFailed(
                    "restore image fetch returned null".into(),
                ))
            } else {
                let image_ref = unsafe { &*image };
                let url = unsafe { image_ref.URL() };
                match unsafe { url.absoluteString() } {
                    Some(s) => Ok(s.to_string()),
                    None => Err(VzError::InstallFailed(
                        "restore image has no download URL".into(),
                    )),
                }
            };
            if let Some(tx) = tx.take() {
                let _ = tx.send(result);
            }
        },
    );

    unsafe {
        VZMacOSRestoreImage::fetchLatestSupportedWithCompletionHandler(&block);
    }

    rx.await
        .map_err(|_| VzError::InstallFailed("IPSW URL fetch callback was never invoked".into()))?
}

/// Generate a new machine identifier and return its serialized data.
fn generate_machine_id_data() -> Result<Vec<u8>, VzError> {
    let machine_id = unsafe { VZMacMachineIdentifier::init(VZMacMachineIdentifier::alloc()) };
    let data = unsafe { machine_id.dataRepresentation() };
    Ok(data.to_vec())
}

/// Resolve the local IPSW file path from the source.
fn resolve_ipsw_path(source: &IpswSource) -> Result<PathBuf, VzError> {
    match source {
        IpswSource::Path(p) => {
            if !p.exists() {
                return Err(VzError::InstallFailed(format!(
                    "IPSW file not found: {}",
                    p.display()
                )));
            }
            Ok(p.clone())
        }
        IpswSource::Latest => {
            // IpswSource::Latest fetches metadata, but VZMacOSInstaller
            // requires a local file URL. The caller must download the IPSW
            // first. This logic belongs in the CLI layer.
            Err(VzError::InstallFailed(
                "IpswSource::Latest requires the IPSW to be downloaded first. \
                 Use IpswSource::Path with a local file instead."
                    .into(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Sparse disk image creation
// ---------------------------------------------------------------------------

/// Create a sparse disk image at the given path.
fn create_sparse_disk_image(path: &Path, size_bytes: u64) -> Result<(), VzError> {
    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            VzError::DiskError(format!(
                "failed to create directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    // Create a sparse file by setting the length without writing data
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| {
            VzError::DiskError(format!(
                "failed to create disk image at {}: {e}",
                path.display()
            ))
        })?;

    file.set_len(size_bytes).map_err(|e| {
        VzError::DiskError(format!(
            "failed to set disk image size to {size_bytes} bytes: {e}"
        ))
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Installation execution (on dispatch queue)
// ---------------------------------------------------------------------------

/// Holds ObjC objects that must live on the dispatch queue.
/// These are not Send/Sync but we guarantee access only from the queue.
struct InstallHandle {
    installer: Retained<VZMacOSInstaller>,
    _vm: Retained<VZVirtualMachine>,
    _delegate: Retained<VMDelegate>,
}

// SAFETY: All access to InstallHandle fields is serialized through the dispatch queue.
unsafe impl Send for InstallHandle {}
// SAFETY: Shared references through Arc are safe because all actual access
// happens on the serial dispatch queue.
unsafe impl Sync for InstallHandle {}

/// Run the macOS installation on the dispatch queue.
///
/// All ObjC objects are created on the queue. Only plain Rust data
/// (byte slices, paths, integers) crosses the thread boundary.
async fn run_installation(
    hw_model_data: &[u8],
    machine_id_data: &[u8],
    aux_path: &Path,
    disk_path: &Path,
    ipsw_path: &Path,
    cpus: usize,
    memory: u64,
) -> Result<(), VzError> {
    let (completion_tx, completion_rx) = oneshot::channel::<Result<(), VzError>>();
    let (state_tx, _state_rx) = tokio::sync::watch::channel(VmState::Stopped);

    let queue = SerialQueue::new("com.vz.install");
    let queue_inner = queue.clone_inner();

    // Clone all data that needs to cross the Send boundary
    let hw_model_bytes = hw_model_data.to_vec();
    let machine_id_bytes = machine_id_data.to_vec();
    let aux_path = aux_path.to_path_buf();
    let disk_path = disk_path.to_path_buf();
    let ipsw_path = ipsw_path.to_path_buf();

    // Create VM, installer, and start installation on the dispatch queue
    let handle: Arc<InstallHandle> = queue
        .dispatch(move || -> Result<Arc<InstallHandle>, VzError> {
            // Reconstruct ObjC objects from serialized data
            let hw_model_nsdata = NSData::with_bytes(&hw_model_bytes);
            let hw_model = unsafe {
                VZMacHardwareModel::initWithDataRepresentation(
                    VZMacHardwareModel::alloc(),
                    &hw_model_nsdata,
                )
            }
            .ok_or_else(|| {
                VzError::InstallFailed("failed to recreate hardware model from data".into())
            })?;

            let machine_id_nsdata = NSData::with_bytes(&machine_id_bytes);
            let machine_id = unsafe {
                VZMacMachineIdentifier::initWithDataRepresentation(
                    VZMacMachineIdentifier::alloc(),
                    &machine_id_nsdata,
                )
            }
            .ok_or_else(|| {
                VzError::InstallFailed(
                    "failed to recreate machine identifier from data".into(),
                )
            })?;

            // Create auxiliary storage
            let aux_url = nsurl_from_path(&aux_path);
            let aux_storage = unsafe {
                VZMacAuxiliaryStorage::initCreatingStorageAtURL_hardwareModel_options_error(
                    VZMacAuxiliaryStorage::alloc(),
                    &aux_url,
                    &hw_model,
                    VZMacAuxiliaryStorageInitializationOptions::AllowOverwrite,
                )
            }
            .map_err(|e| {
                VzError::InstallFailed(format!(
                    "failed to create auxiliary storage at {}: {}",
                    aux_path.display(),
                    e.localizedDescription()
                ))
            })?;

            // Build minimal VZ configuration for installation
            let vz_config = unsafe { VZVirtualMachineConfiguration::new() };
            unsafe {
                vz_config.setCPUCount(cpus);
                vz_config.setMemorySize(memory);
            }

            // Boot loader
            let boot_loader = unsafe { VZMacOSBootLoader::new() };
            unsafe { vz_config.setBootLoader(Some(&boot_loader)) };

            // Platform
            let platform = unsafe { VZMacPlatformConfiguration::new() };
            unsafe {
                platform.setHardwareModel(&hw_model);
                platform.setMachineIdentifier(&machine_id);
                platform.setAuxiliaryStorage(Some(&aux_storage));
            }
            unsafe { vz_config.setPlatform(&platform) };

            // Disk
            let disk_url = nsurl_from_path(&disk_path);
            let disk_attachment = unsafe {
                objc2_virtualization::VZDiskImageStorageDeviceAttachment::initWithURL_readOnly_error(
                    objc2_virtualization::VZDiskImageStorageDeviceAttachment::alloc(),
                    &disk_url,
                    false,
                )
            }
            .map_err(|e| {
                VzError::DiskError(format!(
                    "failed to create disk attachment: {}",
                    e.localizedDescription()
                ))
            })?;
            let block_device = unsafe {
                VZVirtioBlockDeviceConfiguration::initWithAttachment(
                    VZVirtioBlockDeviceConfiguration::alloc(),
                    &disk_attachment,
                )
            };
            let storage_devices =
                objc2_foundation::NSArray::from_retained_slice(&[Retained::into_super(
                    block_device,
                )]);
            unsafe { vz_config.setStorageDevices(&storage_devices) };

            // Graphics device (required for macOS installation)
            let graphics_config = unsafe { VZMacGraphicsDeviceConfiguration::new() };
            let display = unsafe {
                VZMacGraphicsDisplayConfiguration::initWithWidthInPixels_heightInPixels_pixelsPerInch(
                    VZMacGraphicsDisplayConfiguration::alloc(),
                    1920,
                    1200,
                    80,
                )
            };
            let displays = objc2_foundation::NSArray::from_retained_slice(&[display]);
            unsafe { graphics_config.setDisplays(&displays) };
            let graphics_devices = objc2_foundation::NSArray::from_retained_slice(&[
                Retained::into_super(graphics_config),
            ]);
            unsafe { vz_config.setGraphicsDevices(&graphics_devices) };

            // Network device (NAT, needed for activation)
            let nat_attachment = unsafe { VZNATNetworkDeviceAttachment::new() };
            let net_config = unsafe { VZVirtioNetworkDeviceConfiguration::new() };
            unsafe { net_config.setAttachment(Some(&nat_attachment)) };
            let network_devices = objc2_foundation::NSArray::from_retained_slice(&[
                Retained::into_super(net_config),
            ]);
            unsafe { vz_config.setNetworkDevices(&network_devices) };

            // Pointing device (required for macOS)
            let pointing = unsafe { VZUSBScreenCoordinatePointingDeviceConfiguration::new() };
            let pointing_devices = objc2_foundation::NSArray::from_retained_slice(&[
                Retained::into_super(pointing),
            ]);
            unsafe { vz_config.setPointingDevices(&pointing_devices) };

            // Validate
            unsafe { vz_config.validateWithError() }.map_err(|e| {
                VzError::InvalidConfig(format!(
                    "installation VM config validation failed: {}",
                    e.localizedDescription()
                ))
            })?;

            // Create VM
            let vm = unsafe {
                VZVirtualMachine::initWithConfiguration_queue(
                    VZVirtualMachine::alloc(),
                    &vz_config,
                    &queue_inner,
                )
            };

            // Delegate
            let delegate = VMDelegate::new(state_tx);
            unsafe { vm.setDelegate(Some(delegate.as_protocol())) };

            // Create installer
            let ipsw_url = nsurl_from_path(&ipsw_path);
            let installer = unsafe {
                VZMacOSInstaller::initWithVirtualMachine_restoreImageURL(
                    VZMacOSInstaller::alloc(),
                    &vm,
                    &ipsw_url,
                )
            };

            // Start installation with completion handler
            let block = completion_handler_block(completion_tx);
            unsafe { installer.installWithCompletionHandler(&block) };

            Ok(Arc::new(InstallHandle {
                installer,
                _vm: vm,
                _delegate: delegate,
            }))
        })
        .await??;

    // Poll progress while waiting for completion
    info!("macOS installation started, monitoring progress...");

    let poll_handle = Arc::clone(&handle);
    let progress_task = tokio::spawn(async move {
        let mut last_pct: i64 = -1;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Read progress from the installer's NSProgress.
            // NSProgress properties are KVO-observable and thread-safe for reading.
            let progress = unsafe { poll_handle.installer.progress() };
            let fraction = unsafe { progress.fractionCompleted() };
            let pct = (fraction * 100.0) as i64;

            if pct != last_pct {
                info!(progress_pct = pct, "macOS installation progress");
                last_pct = pct;
            }

            if unsafe { progress.isFinished() } || unsafe { progress.isCancelled() } {
                break;
            }
        }
    });

    // Wait for the installation to complete
    let result = completion_rx.await.map_err(|_| {
        VzError::InstallFailed(
            "installation completion handler was never called (dispatch queue may have been dropped)"
                .into(),
        )
    })?;

    // Stop the progress polling task
    progress_task.abort();

    result.map_err(|e| match e {
        VzError::FrameworkError(msg) => VzError::InstallFailed(msg),
        other => other,
    })
}
