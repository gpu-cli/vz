//! Internal FFI bridge layer.
//!
//! Contains the dispatch queue wrapper, async bridging helpers,
//! VMDelegate implementation, and Foundation type conversions.
//!
//! All `unsafe` code in the `vz` crate is contained in this module.
//! This module is `pub(crate)` — it is not part of the public API.

// Items in this module are pub(crate) and consumed by vm.rs and other
// modules as they are implemented. Suppress dead_code during build-up.
#![allow(dead_code)]

use std::cell::Cell;
use std::path::Path;

use block2::RcBlock;
use dispatch2::{DispatchQueue, DispatchQueueAttr, DispatchRetained};
use objc2::rc::Retained;
use objc2::runtime::{NSObjectProtocol, ProtocolObject};
use objc2::{AnyThread, DefinedClass, define_class, msg_send};
use objc2_foundation::{NSArray, NSData, NSError, NSObject, NSString, NSURL};
use objc2_virtualization::{
    VZDiskImageStorageDeviceAttachment, VZGenericPlatformConfiguration, VZLinuxBootLoader,
    VZMacAuxiliaryStorage, VZMacGraphicsDeviceConfiguration, VZMacGraphicsDisplayConfiguration,
    VZMacHardwareModel, VZMacMachineIdentifier, VZMacOSBootLoader, VZMacPlatformConfiguration,
    VZNATNetworkDeviceAttachment, VZSharedDirectory, VZSingleDirectoryShare,
    VZUSBKeyboardConfiguration, VZUSBScreenCoordinatePointingDeviceConfiguration,
    VZVirtioBlockDeviceConfiguration, VZVirtioFileSystemDeviceConfiguration,
    VZVirtioNetworkDeviceConfiguration, VZVirtioSocketDeviceConfiguration, VZVirtualMachine,
    VZVirtualMachineConfiguration, VZVirtualMachineDelegate,
};
use tokio::sync::{oneshot, watch};

use crate::config::{BootLoader, MacPlatformConfig, NetworkConfig, VmConfig};
use crate::error::VzError;
use crate::vm::VmState;

// ---------------------------------------------------------------------------
// Serial Dispatch Queue
// ---------------------------------------------------------------------------

/// A serial dispatch queue for VM operations.
///
/// Apple's Virtualization.framework requires that all `VZVirtualMachine`
/// operations execute on a dedicated serial dispatch queue. This wrapper
/// provides a safe async interface for dispatching work to that queue
/// and bridging results back to tokio.
pub(crate) struct SerialQueue {
    inner: DispatchRetained<DispatchQueue>,
}

impl SerialQueue {
    /// Create a new serial dispatch queue with the given label.
    pub(crate) fn new(label: &str) -> Self {
        let inner = DispatchQueue::new(label, DispatchQueueAttr::SERIAL);
        Self { inner }
    }

    /// Returns a reference to the underlying `DispatchQueue`.
    pub(crate) fn as_raw(&self) -> &DispatchQueue {
        &self.inner
    }

    /// Returns a clone of the underlying dispatch queue (reference-counted).
    ///
    /// Use this when the queue reference needs to be moved into a closure
    /// or sent across thread boundaries.
    pub(crate) fn clone_inner(&self) -> DispatchRetained<DispatchQueue> {
        self.inner.clone()
    }

    /// Dispatch a closure to the serial queue and await its result.
    ///
    /// This bridges GCD's dispatch model to tokio's async model:
    /// 1. Creates a `tokio::sync::oneshot` channel
    /// 2. Dispatches the closure onto the GCD serial queue via `exec_async`
    /// 3. The closure executes on the queue, sends its result through the channel
    /// 4. The caller awaits the oneshot receiver
    ///
    /// The tokio task yields while waiting, so no threads are blocked.
    pub(crate) async fn dispatch<F, R>(&self, f: F) -> Result<R, VzError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.inner.exec_async(move || {
            let result = f();
            // Receiver may have been dropped if the caller cancelled.
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| {
            VzError::FrameworkError("dispatch queue dropped before completing operation".into())
        })
    }
}

impl std::fmt::Debug for SerialQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerialQueue").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// VM Delegate (ObjC class via define_class!)
// ---------------------------------------------------------------------------

/// Ivar storage for `VMDelegate`.
///
/// Uses `Cell<Option<...>>` because ObjC delegate methods receive `&self`
/// (not `&mut self`), but we need to mutate (send through the channel).
/// Since the delegate executes exclusively on the VM's serial dispatch
/// queue, `Cell` is sufficient — no locking needed.
pub(crate) struct VMDelegateIvars {
    state_tx: Cell<Option<watch::Sender<VmState>>>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements.
    // VMDelegate does not implement Drop.
    #[unsafe(super(NSObject))]
    #[ivars = VMDelegateIvars]
    #[name = "VZRustVMDelegate"]
    pub(crate) struct VMDelegate;

    unsafe impl NSObjectProtocol for VMDelegate {}

    unsafe impl VZVirtualMachineDelegate for VMDelegate {
        /// Called when the guest OS initiates a clean shutdown.
        #[unsafe(method(guestDidStopVirtualMachine:))]
        fn guest_did_stop(&self, _vm: &VZVirtualMachine) {
            if let Some(tx) = self.ivars().state_tx.take() {
                let _ = tx.send(VmState::Stopped);
                self.ivars().state_tx.set(Some(tx));
            }
        }

        /// Called when the VM stops due to an error.
        #[unsafe(method(virtualMachine:didStopWithError:))]
        fn vm_did_stop_with_error(&self, _vm: &VZVirtualMachine, error: &NSError) {
            if let Some(tx) = self.ivars().state_tx.take() {
                let msg = error.localizedDescription().to_string();
                let _ = tx.send(VmState::Error(msg));
                self.ivars().state_tx.set(Some(tx));
            }
        }
    }
);

impl VMDelegate {
    /// Create a new VMDelegate that forwards state changes to the given sender.
    pub(crate) fn new(tx: watch::Sender<VmState>) -> Retained<Self> {
        let ivars = VMDelegateIvars {
            state_tx: Cell::new(Some(tx)),
        };
        let this = Self::alloc().set_ivars(ivars);
        unsafe { msg_send![super(this), init] }
    }

    /// Convert to a protocol object reference suitable for `setDelegate:`.
    pub(crate) fn as_protocol(&self) -> &ProtocolObject<dyn VZVirtualMachineDelegate> {
        ProtocolObject::from_ref(self)
    }
}

// ---------------------------------------------------------------------------
// Async Bridging: ObjC Completion Handlers → Tokio Futures
// ---------------------------------------------------------------------------

/// Create an `RcBlock` completion handler that bridges an ObjC error callback
/// to a `oneshot::Receiver`.
///
/// The returned block has type `dyn Fn(*mut NSError)`, which is the signature
/// used by most Virtualization.framework completion handlers.
///
/// The `Cell<Option<Sender>>` pattern is required because `RcBlock::new`
/// needs `Fn` (not `FnOnce`), but a oneshot sender can only be used once.
pub(crate) fn completion_handler_block(
    tx: oneshot::Sender<Result<(), VzError>>,
) -> RcBlock<dyn Fn(*mut NSError)> {
    let tx = Cell::new(Some(tx));
    RcBlock::new(move |error: *mut NSError| {
        let result = if error.is_null() {
            Ok(())
        } else {
            // SAFETY: We just checked the pointer is non-null, and the
            // framework guarantees the NSError is valid within the callback.
            let err = unsafe { &*error };
            Err(ns_error_to_vz_error(err))
        };
        if let Some(tx) = tx.take() {
            let _ = tx.send(result);
        }
    })
}

/// Bridge an ObjC completion handler operation to an async Result.
///
/// This creates a oneshot channel, builds a completion handler block,
/// and returns both the receiver (for awaiting) and the block (for
/// passing to the ObjC method).
#[allow(clippy::type_complexity)]
pub(crate) fn bridge_completion_handler() -> (
    oneshot::Receiver<Result<(), VzError>>,
    RcBlock<dyn Fn(*mut NSError)>,
) {
    let (tx, rx) = oneshot::channel();
    let block = completion_handler_block(tx);
    (rx, block)
}

/// Await a bridged completion handler result.
///
/// Handles the case where the oneshot sender is dropped without sending
/// (which would indicate the dispatch queue was destroyed).
pub(crate) async fn await_completion(
    rx: oneshot::Receiver<Result<(), VzError>>,
) -> Result<(), VzError> {
    rx.await.map_err(|_| {
        VzError::FrameworkError(
            "completion handler was never called (dispatch queue may have been dropped)".into(),
        )
    })?
}

// ---------------------------------------------------------------------------
// Foundation Type Conversions
// ---------------------------------------------------------------------------

/// Convert an `NSError` to a `VzError::FrameworkError`.
pub(crate) fn ns_error_to_vz_error(error: &NSError) -> VzError {
    let description = error.localizedDescription().to_string();
    let code = error.code();
    let domain = error.domain().to_string();
    VzError::FrameworkError(format!("{description} ({domain}:{code})"))
}

/// Convert a `Path` to an `NSURL` file URL.
pub(crate) fn nsurl_from_path(path: &Path) -> Retained<NSURL> {
    let path_str = NSString::from_str(&path.to_string_lossy());
    NSURL::initFileURLWithPath(NSURL::alloc(), &path_str)
}

/// Convert an `NSString` reference to a Rust `String`.
pub(crate) fn nsstring_to_string(s: &NSString) -> String {
    s.to_string()
}

// ---------------------------------------------------------------------------
// ObjC Configuration Builder
// ---------------------------------------------------------------------------

/// Build a `VZVirtualMachineConfiguration` from a `VmConfig`.
///
/// This constructs all ObjC configuration objects (boot loader, platform,
/// storage devices, network, VirtioFS, vsock) and validates the result.
///
/// # Safety
///
/// All unsafe calls are contained here. The returned configuration is valid
/// and ready to create a `VZVirtualMachine`.
pub(crate) fn build_objc_config(
    config: &VmConfig,
) -> Result<Retained<VZVirtualMachineConfiguration>, VzError> {
    // SAFETY: VZVirtualMachineConfiguration::new() returns a default-initialized config.
    let vz_config = unsafe { VZVirtualMachineConfiguration::new() };

    // CPU and memory
    // SAFETY: These setters are safe to call with valid values.
    unsafe {
        vz_config.setCPUCount(config.cpus as usize);
        vz_config.setMemorySize(config.memory_bytes);
    }

    // Boot loader and platform
    match &config.boot_loader {
        BootLoader::MacOS => {
            // SAFETY: VZMacOSBootLoader::new() creates a valid boot loader.
            let boot_loader = unsafe { VZMacOSBootLoader::new() };
            // SAFETY: setBootLoader accepts any VZBootLoader subclass.
            unsafe { vz_config.setBootLoader(Some(&boot_loader)) };

            // Set up Mac platform configuration
            if let Some(ref mac_platform) = config.mac_platform {
                let platform = build_mac_platform(mac_platform)?;
                // SAFETY: setPlatform accepts any VZPlatformConfiguration subclass.
                unsafe { vz_config.setPlatform(&platform) };
            }
        }
        BootLoader::Linux {
            kernel,
            initrd,
            cmdline,
        } => {
            let kernel_url = nsurl_from_path(kernel);

            // SAFETY: initWithKernelURL creates a Linux boot loader with a kernel path.
            let boot_loader = unsafe {
                VZLinuxBootLoader::initWithKernelURL(VZLinuxBootLoader::alloc(), &kernel_url)
            };

            let cmdline = NSString::from_str(cmdline);
            // SAFETY: setCommandLine copies the command line string.
            unsafe { boot_loader.setCommandLine(&cmdline) };

            if let Some(initrd) = initrd {
                let initrd_url = nsurl_from_path(initrd);
                // SAFETY: setInitialRamdiskURL accepts an optional initrd URL.
                unsafe { boot_loader.setInitialRamdiskURL(Some(&initrd_url)) };
            }

            // SAFETY: setBootLoader accepts any VZBootLoader subclass.
            unsafe { vz_config.setBootLoader(Some(&boot_loader)) };

            // Linux guests require a generic platform configuration.
            // SAFETY: VZGenericPlatformConfiguration::new() creates a valid generic platform.
            let platform = unsafe { VZGenericPlatformConfiguration::new() };
            // SAFETY: setPlatform accepts any VZPlatformConfiguration subclass.
            unsafe { vz_config.setPlatform(&platform) };
        }
    }

    // Storage devices (disk images)
    if let Some(disk_path) = &config.disk_path {
        let disk_url = nsurl_from_path(disk_path);
        // SAFETY: initWithURL_readOnly_error validates the disk image path.
        let disk_attachment = unsafe {
            VZDiskImageStorageDeviceAttachment::initWithURL_readOnly_error(
                VZDiskImageStorageDeviceAttachment::alloc(),
                &disk_url,
                false,
            )
        }
        .map_err(|e| VzError::DiskError(ns_error_to_string(&e)))?;

        // SAFETY: initWithAttachment creates a VirtioBlock device with the attachment.
        let block_device = unsafe {
            VZVirtioBlockDeviceConfiguration::initWithAttachment(
                VZVirtioBlockDeviceConfiguration::alloc(),
                &disk_attachment,
            )
        };

        // SAFETY: NSArray::from_retained_slice creates an array from a slice of retained objects.
        let storage_devices = NSArray::from_retained_slice(&[Retained::into_super(block_device)]);
        // SAFETY: setStorageDevices sets the VM's storage configuration.
        unsafe { vz_config.setStorageDevices(&storage_devices) };
    }

    // Network
    match &config.network {
        NetworkConfig::Nat => {
            // SAFETY: VZNATNetworkDeviceAttachment::new() creates a NAT attachment.
            let nat_attachment = unsafe { VZNATNetworkDeviceAttachment::new() };
            // SAFETY: VZVirtioNetworkDeviceConfiguration::new() creates a default net config.
            let net_config = unsafe { VZVirtioNetworkDeviceConfiguration::new() };
            // SAFETY: setAttachment sets the network attachment on the config.
            unsafe { net_config.setAttachment(Some(&nat_attachment)) };

            // Use a deterministic MAC address so save/restore configs match.
            // VZVirtioNetworkDeviceConfiguration::new() generates a random MAC,
            // which breaks restore (config mismatch → "invalid argument").
            let mac = unsafe {
                objc2_virtualization::VZMACAddress::initWithString(
                    objc2_virtualization::VZMACAddress::alloc(),
                    &NSString::from_str("76:c4:f2:a0:00:01"),
                )
            };
            if let Some(mac) = mac {
                unsafe { net_config.setMACAddress(&mac) };
            }

            let net_devices = NSArray::from_retained_slice(&[Retained::into_super(net_config)]);
            // SAFETY: setNetworkDevices sets the VM's network configuration.
            unsafe { vz_config.setNetworkDevices(&net_devices) };
        }
        NetworkConfig::None => {
            // No network devices
        }
    }

    // Graphics device (required by macOS guests for proper operation, even headless)
    if matches!(config.boot_loader, BootLoader::MacOS) {
        // SAFETY: These create a minimal Mac graphics config with a single display.
        let graphics_config = unsafe { VZMacGraphicsDeviceConfiguration::new() };
        let display = unsafe {
            VZMacGraphicsDisplayConfiguration::initWithWidthInPixels_heightInPixels_pixelsPerInch(
                VZMacGraphicsDisplayConfiguration::alloc(),
                1920,
                1200,
                80,
            )
        };
        let displays = NSArray::from_retained_slice(&[display]);
        unsafe { graphics_config.setDisplays(&displays) };
        let graphics_devices =
            NSArray::from_retained_slice(&[Retained::into_super(graphics_config)]);
        unsafe { vz_config.setGraphicsDevices(&graphics_devices) };

        // Keyboard and pointing device
        let keyboard = unsafe { VZUSBKeyboardConfiguration::new() };
        let keyboards = NSArray::from_retained_slice(&[Retained::into_super(keyboard)]);
        unsafe { vz_config.setKeyboards(&keyboards) };

        let pointing = unsafe { VZUSBScreenCoordinatePointingDeviceConfiguration::new() };
        let pointing_devices = NSArray::from_retained_slice(&[Retained::into_super(pointing)]);
        unsafe { vz_config.setPointingDevices(&pointing_devices) };
    }

    // Vsock
    if config.vsock {
        // SAFETY: VZVirtioSocketDeviceConfiguration::new() creates a vsock device config.
        let vsock_config = unsafe { VZVirtioSocketDeviceConfiguration::new() };
        let socket_devices = NSArray::from_retained_slice(&[Retained::into_super(vsock_config)]);
        // SAFETY: setSocketDevices sets the VM's socket configuration.
        unsafe { vz_config.setSocketDevices(&socket_devices) };
    }

    // VirtioFS shared directories
    if !config.shared_dirs.is_empty() {
        let mut fs_devices: Vec<
            Retained<objc2_virtualization::VZDirectorySharingDeviceConfiguration>,
        > = Vec::new();
        for shared_dir in &config.shared_dirs {
            let source_url = nsurl_from_path(&shared_dir.source);
            // SAFETY: initWithURL_readOnly creates a shared directory descriptor.
            let shared = unsafe {
                VZSharedDirectory::initWithURL_readOnly(
                    VZSharedDirectory::alloc(),
                    &source_url,
                    shared_dir.read_only,
                )
            };
            // SAFETY: initWithDirectory wraps the shared directory in a share.
            let share = unsafe {
                VZSingleDirectoryShare::initWithDirectory(VZSingleDirectoryShare::alloc(), &shared)
            };
            let tag = NSString::from_str(&shared_dir.tag);
            // SAFETY: initWithTag creates a VirtioFS device configuration with the tag.
            let fs_config = unsafe {
                VZVirtioFileSystemDeviceConfiguration::initWithTag(
                    VZVirtioFileSystemDeviceConfiguration::alloc(),
                    &tag,
                )
            };
            // SAFETY: setShare sets the directory share on the device config.
            unsafe { fs_config.setShare(Some(&share)) };
            fs_devices.push(Retained::into_super(fs_config));
        }
        let fs_array = NSArray::from_retained_slice(&fs_devices);
        // SAFETY: setDirectorySharingDevices sets the VM's shared directory configuration.
        unsafe { vz_config.setDirectorySharingDevices(&fs_array) };
    }

    // Validate the configuration
    // SAFETY: validateWithError checks all configuration invariants.
    unsafe { vz_config.validateWithError() }
        .map_err(|e| VzError::InvalidConfig(ns_error_to_string(&e)))?;

    // Validate save/restore support
    match unsafe { vz_config.validateSaveRestoreSupportWithError() } {
        Ok(()) => tracing::debug!("VM config supports save/restore"),
        Err(e) => tracing::warn!(
            error = %ns_error_to_string(&e),
            "VM config does NOT support save/restore"
        ),
    }

    Ok(vz_config)
}

/// Build a `VZMacPlatformConfiguration` from persisted platform files.
fn build_mac_platform(
    config: &MacPlatformConfig,
) -> Result<Retained<VZMacPlatformConfiguration>, VzError> {
    // Load hardware model from file
    let hw_model_data = std::fs::read(&config.hardware_model_path).map_err(|e| {
        VzError::InvalidConfig(format!(
            "failed to read hardware model from {}: {e}",
            config.hardware_model_path.display()
        ))
    })?;
    let hw_model_nsdata = NSData::with_bytes(&hw_model_data);
    // SAFETY: initWithDataRepresentation creates a hardware model from serialized data.
    let hw_model = unsafe {
        VZMacHardwareModel::initWithDataRepresentation(
            VZMacHardwareModel::alloc(),
            &hw_model_nsdata,
        )
    }
    .ok_or_else(|| {
        VzError::InvalidConfig(format!(
            "invalid hardware model data in {}",
            config.hardware_model_path.display()
        ))
    })?;

    // Load machine identifier from file
    let machine_id_data = std::fs::read(&config.machine_identifier_path).map_err(|e| {
        VzError::InvalidConfig(format!(
            "failed to read machine identifier from {}: {e}",
            config.machine_identifier_path.display()
        ))
    })?;
    let machine_id_nsdata = NSData::with_bytes(&machine_id_data);
    // SAFETY: initWithDataRepresentation creates a machine ID from serialized data.
    let machine_id = unsafe {
        VZMacMachineIdentifier::initWithDataRepresentation(
            VZMacMachineIdentifier::alloc(),
            &machine_id_nsdata,
        )
    }
    .ok_or_else(|| {
        VzError::InvalidConfig(format!(
            "invalid machine identifier data in {}",
            config.machine_identifier_path.display()
        ))
    })?;

    // Load auxiliary storage from existing file
    let aux_url = nsurl_from_path(&config.auxiliary_storage_path);
    // SAFETY: initWithURL loads existing auxiliary storage.
    let aux_storage =
        unsafe { VZMacAuxiliaryStorage::initWithURL(VZMacAuxiliaryStorage::alloc(), &aux_url) };

    // Build the platform configuration
    // SAFETY: VZMacPlatformConfiguration::new() creates a default Mac platform config.
    let platform = unsafe { VZMacPlatformConfiguration::new() };
    // SAFETY: These setters configure the platform with validated data.
    unsafe {
        platform.setHardwareModel(&hw_model);
        platform.setMachineIdentifier(&machine_id);
        platform.setAuxiliaryStorage(Some(&aux_storage));
    }

    Ok(platform)
}

/// Create a `VZVirtualMachine` on the given dispatch queue.
///
/// Must be called from the dispatch queue itself.
pub(crate) fn create_vm_on_queue(
    config: &VZVirtualMachineConfiguration,
    queue: &DispatchQueue,
) -> Retained<VZVirtualMachine> {
    // SAFETY: initWithConfiguration_queue creates a VM on the specified queue.
    // This must be called from the queue itself (which our caller ensures).
    unsafe {
        VZVirtualMachine::initWithConfiguration_queue(VZVirtualMachine::alloc(), config, queue)
    }
}

/// Convert an `NSError` to a descriptive error string.
fn ns_error_to_string(error: &NSError) -> String {
    let description = error.localizedDescription().to_string();
    let code = error.code();
    let domain = error.domain().to_string();
    format!("{description} ({domain}:{code})")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- SerialQueue tests --

    #[tokio::test]
    async fn dispatch_returns_value() {
        let queue = SerialQueue::new("com.vz.test-queue");
        let result = queue.dispatch(|| 42).await;
        assert_eq!(result.ok(), Some(42));
    }

    #[tokio::test]
    async fn dispatch_preserves_execution_order() {
        let queue = SerialQueue::new("com.vz.test-order");

        let r1 = queue.dispatch(|| 1).await;
        let r2 = queue.dispatch(|| 2).await;
        let r3 = queue.dispatch(|| 3).await;

        assert_eq!(r1.ok(), Some(1));
        assert_eq!(r2.ok(), Some(2));
        assert_eq!(r3.ok(), Some(3));
    }

    #[tokio::test]
    async fn dispatch_with_string_result() {
        let queue = SerialQueue::new("com.vz.test-string");
        let result = queue.dispatch(|| String::from("hello from dispatch")).await;
        assert_eq!(result.ok(), Some(String::from("hello from dispatch")));
    }

    #[test]
    fn serial_queue_debug() {
        let queue = SerialQueue::new("com.vz.test-debug");
        let debug_str = format!("{:?}", queue);
        assert!(debug_str.contains("SerialQueue"));
    }

    // -- VMDelegate tests --

    #[test]
    fn vm_delegate_creates_successfully() {
        let (tx, _rx) = watch::channel(VmState::Stopped);
        let delegate = VMDelegate::new(tx);
        // Just verify it creates without crashing
        let _ = delegate.as_protocol();
    }

    #[test]
    fn vm_delegate_state_channel_connected() {
        let (tx, rx) = watch::channel(VmState::Stopped);
        let _delegate = VMDelegate::new(tx);
        // The receiver should have the initial value
        assert_eq!(*rx.borrow(), VmState::Stopped);
    }

    // -- Async bridging tests --

    #[tokio::test]
    async fn completion_handler_success() {
        let (rx, block) = bridge_completion_handler();

        // Simulate a successful ObjC completion handler call
        block.call((std::ptr::null_mut(),));

        let result = await_completion(rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn completion_handler_dropped_sender() {
        let (tx, rx) = oneshot::channel::<Result<(), VzError>>();
        // Drop the sender without sending
        drop(tx);

        let result = await_completion(rx).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, VzError::FrameworkError(msg) if msg.contains("completion handler")));
    }

    // -- Foundation conversion tests --

    #[test]
    fn nsurl_from_path_roundtrip() {
        let path = Path::new("/tmp/test-vm-state.vzsave");
        let url = nsurl_from_path(path);
        let url_path = url.path();
        if let Some(p) = url_path {
            assert_eq!(p.to_string(), "/tmp/test-vm-state.vzsave");
        }
    }

    #[test]
    fn nsstring_conversion() {
        let ns = NSString::from_str("hello from Rust");
        let rust_str = nsstring_to_string(&ns);
        assert_eq!(rust_str, "hello from Rust");
    }

    #[test]
    fn nsstring_empty() {
        let ns = NSString::from_str("");
        let rust_str = nsstring_to_string(&ns);
        assert_eq!(rust_str, "");
    }

    #[test]
    fn nsstring_unicode() {
        let ns = NSString::from_str("hello 🌍 世界");
        let rust_str = nsstring_to_string(&ns);
        assert_eq!(rust_str, "hello 🌍 世界");
    }
}
