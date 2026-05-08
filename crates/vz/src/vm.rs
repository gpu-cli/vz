//! Virtual machine lifecycle management.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use objc2::AnyThread;
use objc2::rc::Retained;
use objc2_virtualization::{VZVirtualMachine, VZVirtualMachineState};
use tokio::sync::watch;

use crate::bridge::{
    self, SerialQueue, VMDelegate, await_completion, build_objc_config, completion_handler_block,
    nsurl_from_path,
};
use crate::config::VmConfig;
use crate::error::VzError;
use crate::vsock::{SendableConnection, VsockListener, VsockStream};

/// Global counter for unique VM dispatch queue labels.
static VM_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Translate Apple's `VZVirtualMachineState` into our [`VmState`].
///
/// Used by `Vm::push_current_state` to mirror framework state into the
/// watch after every API operation completes. The mapping is total
/// across the documented Apple values; any unknown raw value (forward
/// compat — Apple has added Saving/Restoring at macOS 14) is preserved
/// as `VmState::Error("unknown VZVirtualMachineState(<n>)")` rather
/// than silently dropped, so a future SDK addition shows up in logs
/// instead of producing a phantom Stopped.
///
/// Note: `VZVirtualMachineState::Error` is intentionally translated to
/// a *placeholder* `VmState::Error` with no description. The detailed
/// `NSError` description is produced by the
/// `virtualMachine:didStopWithError:` delegate callback (see
/// [`crate::bridge::VMDelegate`]); it lands in the watch immediately
/// after this placeholder. Callers that need the description should
/// observe the next `changed()` push, not just borrow once.
pub(crate) fn vz_state_to_vm_state(s: VZVirtualMachineState) -> VmState {
    match s {
        VZVirtualMachineState::Stopped => VmState::Stopped,
        VZVirtualMachineState::Starting => VmState::Starting,
        VZVirtualMachineState::Running => VmState::Running,
        VZVirtualMachineState::Pausing => VmState::Pausing,
        VZVirtualMachineState::Paused => VmState::Paused,
        VZVirtualMachineState::Resuming => VmState::Resuming,
        VZVirtualMachineState::Stopping => VmState::Stopping,
        VZVirtualMachineState::Saving => VmState::Saving,
        VZVirtualMachineState::Restoring => VmState::Restoring,
        VZVirtualMachineState::Error => VmState::Error(String::new()),
        other => VmState::Error(format!("unknown VZVirtualMachineState({})", other.0)),
    }
}

/// The state of a virtual machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmState {
    /// VM is stopped and not running.
    Stopped,
    /// VM is in the process of starting.
    Starting,
    /// VM is running normally.
    Running,
    /// VM is in the process of pausing.
    Pausing,
    /// VM is paused (frozen in memory).
    Paused,
    /// VM is in the process of resuming from paused state.
    Resuming,
    /// VM is in the process of stopping.
    Stopping,
    /// VM state is being saved to disk (macOS 14+).
    Saving,
    /// VM state is being restored from disk (macOS 14+).
    Restoring,
    /// VM stopped due to an error. Contains the error description.
    Error(String),
}

// ---------------------------------------------------------------------------
// Thread-safety wrapper for VZVirtualMachine
// ---------------------------------------------------------------------------

/// Holds the `VZVirtualMachine` and its `VMDelegate` together.
///
/// Both must live on the dispatch queue and neither is `Send`/`Sync`.
/// We provide `Send + Sync` impls because all access is serialized
/// through the serial dispatch queue.
///
/// The delegate must be retained here to prevent deallocation while
/// the VM holds a weak reference to it.
struct VmHandle {
    vm: Retained<VZVirtualMachine>,
    _delegate: Retained<VMDelegate>,
}

// SAFETY: All access to VmHandle fields is serialized through the dispatch queue.
// The Vm struct guarantees this by only accessing the VM through queue.dispatch().
unsafe impl Send for VmHandle {}
// SAFETY: Shared references through Arc are safe because all actual access
// happens on the serial dispatch queue.
unsafe impl Sync for VmHandle {}

/// A macOS or Linux virtual machine.
///
/// Wraps `VZVirtualMachine` from Apple's Virtualization.framework.
/// All ObjC calls are dispatched to an internal serial queue, so
/// lifecycle methods are safe to call from any tokio task.
pub struct Vm {
    /// The ObjC virtual machine and delegate, wrapped for thread safety.
    handle: Arc<VmHandle>,
    /// Serial dispatch queue for all VM operations.
    queue: SerialQueue,
    /// Sender for VM state changes. The matching `Receiver`
    /// distributed by [`Vm::state_stream`] is fed by two sources:
    ///
    /// 1. **API-driven transitions** — every `start` / `pause` / `resume`
    ///    completion handler synchronously re-reads
    ///    `[VZVirtualMachine state]` (the framework's authoritative
    ///    value) from inside the dispatch queue and pushes the
    ///    translated [`VmState`] before returning to the caller.
    /// 2. **Delegate-driven terminal transitions** —
    ///    `guestDidStopVirtualMachine:` and
    ///    `virtualMachine:didStopWithError:` push `Stopped` /
    ///    `Error(_)` from the delegate.
    ///
    /// Without source #1 the watcher would only ever observe the
    /// initial `Stopped` (set in `watch::channel`) followed by a final
    /// `Stopped` / `Error(_)` — there'd be no way to distinguish "VM
    /// has not yet started" from "VM ran and halted." That ambiguity
    /// caused VRT-yl5l (boon prep VMs that "succeeded" in <1ms with an
    /// all-zeros state.ext4 because callers' `wait_for_halt` saw the
    /// initial `Stopped` and returned immediately).
    state_tx: watch::Sender<VmState>,
    /// Receiver for VM state changes (fed by the delegate).
    state_rx: watch::Receiver<VmState>,
    /// The validated configuration used to create this VM.
    _config: VmConfig,
}

impl Vm {
    /// Create a new VM from a validated configuration.
    ///
    /// Constructs all ObjC configuration objects, creates the dispatch queue,
    /// sets up the VMDelegate, and returns a ready-to-start VM.
    ///
    /// The VM is created but not started -- call [`start`](Self::start) to boot it.
    pub async fn create(config: VmConfig) -> Result<Self, VzError> {
        // Create a unique serial dispatch queue for this VM
        let vm_id = VM_COUNTER.fetch_add(1, Ordering::Relaxed);
        let queue = SerialQueue::new(&format!("com.vz.vm-{vm_id}"));

        // Set up the state channel. The delegate gets one clone for the
        // terminal `guestDidStop` / `didStopWithError` callbacks; we
        // keep our own clone on `Vm` so API calls (`start` / `pause` /
        // `resume`) can also push transitions after they complete.
        // Without that second clone the watch never observes anything
        // between the initial `Stopped` and the terminal state — see
        // VRT-yl5l for the failure mode.
        let (state_tx, state_rx) = watch::channel(VmState::Stopped);
        let delegate_state_tx = state_tx.clone();

        // Build all ObjC objects AND create the VM on the dispatch queue.
        // ObjC objects are not Send, so everything must be created on the queue.
        let config_clone = config.clone();
        let queue_inner = queue.clone_inner();

        let handle = queue
            .dispatch(move || -> Result<Arc<VmHandle>, VzError> {
                // Build all ObjC configuration objects
                let vz_config = build_objc_config(&config_clone)?;

                // Create the VM on this queue
                // SAFETY: initWithConfiguration_queue creates a VM bound to the given queue.
                // We are executing on that queue right now.
                let vm = unsafe {
                    VZVirtualMachine::initWithConfiguration_queue(
                        VZVirtualMachine::alloc(),
                        &vz_config,
                        &queue_inner,
                    )
                };

                // Create the delegate on this queue
                let delegate = VMDelegate::new(delegate_state_tx);

                // Set the delegate on the VM (weak reference)
                // SAFETY: setDelegate must be called on the VM's queue (we are on it).
                unsafe { vm.setDelegate(Some(delegate.as_protocol())) };

                Ok(Arc::new(VmHandle {
                    vm,
                    _delegate: delegate,
                }))
            })
            .await??;

        Ok(Self {
            handle,
            queue,
            state_tx,
            state_rx,
            _config: config,
        })
    }

    /// Re-read Apple's `[VZVirtualMachine state]` property from the
    /// dispatch queue and push the translated value into the watch.
    ///
    /// Called by API methods (`start` / `pause` / `resume`) immediately
    /// after their completion handler fires — the framework's state
    /// property reflects the post-transition state by the time
    /// completion is invoked, so reading it here is the cheapest way to
    /// keep the watch in sync without subscribing to KVO.
    async fn push_current_state(&self) {
        let handle = Arc::clone(&self.handle);
        let state_tx = self.state_tx.clone();
        // Best-effort — if the dispatch queue is gone (only happens if
        // the Vm is being dropped), the watch has likely already been
        // closed too.
        let _ = self
            .queue
            .dispatch(move || {
                // SAFETY: `state` is a queue-bound property on
                // VZVirtualMachine; we are executing on the VM's queue.
                let apple_state = unsafe { handle.vm.state() };
                let _ = state_tx.send(vz_state_to_vm_state(apple_state));
            })
            .await;
    }

    /// Start (cold boot) the VM.
    ///
    /// The VM must be in `Stopped` or `Error` state.
    pub async fn start(&self) -> Result<(), VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                // SAFETY: startWithCompletionHandler must be called on the VM's queue.
                unsafe { handle.vm.startWithCompletionHandler(&block) };
            })
            .await?;

        let result = await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::StartFailed(msg),
            other => other,
        });
        self.push_current_state().await;
        result
    }

    /// Pause the VM (freeze execution, keep state in memory).
    ///
    /// The VM must be in `Running` state.
    pub async fn pause(&self) -> Result<(), VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                // SAFETY: pauseWithCompletionHandler must be called on the VM's queue.
                unsafe { handle.vm.pauseWithCompletionHandler(&block) };
            })
            .await?;

        let result = await_completion(rx).await;
        self.push_current_state().await;
        result
    }

    /// Resume a paused VM.
    ///
    /// The VM must be in `Paused` state.
    pub async fn resume(&self) -> Result<(), VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                // SAFETY: resumeWithCompletionHandler must be called on the VM's queue.
                unsafe { handle.vm.resumeWithCompletionHandler(&block) };
            })
            .await?;

        let result = await_completion(rx).await;
        self.push_current_state().await;
        result
    }

    /// Stop the VM (equivalent to pulling the power cord).
    ///
    /// This is a destructive operation. The guest does not get a chance
    /// to shut down cleanly. Use [`request_stop`](Self::request_stop) for graceful shutdown.
    pub async fn stop(&self) -> Result<(), VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                // SAFETY: stopWithCompletionHandler must be called on the VM's queue.
                unsafe { handle.vm.stopWithCompletionHandler(&block) };
            })
            .await?;

        let result = await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::StopFailed(msg),
            other => other,
        });
        self.push_current_state().await;
        result
    }

    /// Request a graceful guest shutdown.
    ///
    /// Sends a power button event. The guest OS decides how to handle it.
    /// This method returns immediately -- use [`state_stream`](Self::state_stream)
    /// to observe when the VM actually stops.
    pub async fn request_stop(&self) -> Result<(), VzError> {
        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                // SAFETY: requestStopWithError must be called on the VM's queue.
                unsafe { handle.vm.requestStopWithError() }
                    .map_err(|e| VzError::StopFailed(bridge::ns_error_to_vz_error(&e).to_string()))
            })
            .await?
    }

    /// Save full VM state to disk. VM must be paused first.
    ///
    /// Requires macOS 14 (Sonoma) or later. The saved state file is
    /// hardware-encrypted and tied to this Mac + user account.
    pub async fn save_state(&self, path: &Path) -> Result<(), VzError> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(VzError::SaveFailed(format!(
                    "failed to remove existing save file {}: {error}",
                    path.display()
                )));
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let path = path.to_path_buf();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                let save_url = nsurl_from_path(&path);
                // SAFETY: saveMachineStateToURL_completionHandler must be called on the VM's queue.
                unsafe {
                    handle
                        .vm
                        .saveMachineStateToURL_completionHandler(&save_url, &block)
                };
            })
            .await?;

        await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::SaveFailed(msg),
            other => other,
        })
    }

    /// Restore VM from a previously saved state file.
    ///
    /// Must use the same VmConfig that was used when the state was saved.
    /// After restoration, the VM will be in `Paused` state.
    /// Call [`resume`](Self::resume) to continue execution.
    pub async fn restore_state(&self, path: &Path) -> Result<(), VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let path = path.to_path_buf();

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                let block = completion_handler_block(tx);
                let restore_url = nsurl_from_path(&path);
                // SAFETY: restoreMachineStateFromURL_completionHandler must be called on the VM's queue.
                unsafe {
                    handle
                        .vm
                        .restoreMachineStateFromURL_completionHandler(&restore_url, &block)
                };
            })
            .await?;

        await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::RestoreFailed(msg),
            other => other,
        })
    }

    /// Capture checkpoint state to disk.
    ///
    /// Runtime V2 currently uses VM save-state mechanics for checkpoint
    /// persistence. Class-level semantics are enforced at the caller layer.
    pub async fn create_checkpoint(&self, path: &Path) -> Result<(), VzError> {
        self.save_state(path).await
    }

    /// Restore a checkpoint state from disk.
    ///
    /// After restoration, the VM is paused; callers should explicitly resume.
    pub async fn restore_checkpoint(&self, path: &Path) -> Result<(), VzError> {
        self.restore_state(path).await
    }

    /// Fork a checkpoint artifact into a new checkpoint lineage branch.
    pub async fn fork_checkpoint(source: &Path, destination: &Path) -> Result<(), VzError> {
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| VzError::DiskError(format!("create checkpoint dir: {err}")))?;
        }
        tokio::fs::copy(source, destination)
            .await
            .map_err(|err| VzError::DiskError(format!("fork checkpoint copy failed: {err}")))?;
        Ok(())
    }

    /// Connect to the guest over vsock on the given port.
    ///
    /// Returns a bidirectional async byte stream.
    /// Requires vsock to be enabled in the VM configuration.
    pub async fn vsock_connect(&self, port: u32) -> Result<VsockStream, VzError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::cell::Cell::new(Some(tx));

        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                // Get the first socket device from the VM
                let socket_devices = unsafe { handle.vm.socketDevices() };
                if socket_devices.is_empty() {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(VzError::VsockFailed {
                            port,
                            reason: "no vsock device configured on this VM".into(),
                        }));
                    }
                    return;
                }

                // Downcast from VZSocketDevice to VZVirtioSocketDevice
                let device_retained = socket_devices.to_vec().into_iter().next();
                let Some(device) = device_retained else {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(VzError::VsockFailed {
                            port,
                            reason: "failed to get vsock device".into(),
                        }));
                    }
                    return;
                };
                let Ok(virtio_device) =
                    Retained::downcast::<objc2_virtualization::VZVirtioSocketDevice>(device)
                else {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(VzError::VsockFailed {
                            port,
                            reason: "socket device is not a VirtioSocketDevice".into(),
                        }));
                    }
                    return;
                };

                // Connect using the completion handler pattern.
                // Send the raw connection through the channel — VsockStream
                // creation must happen on the tokio thread (AsyncFd needs a reactor).
                let block = block2::RcBlock::new(
                    move |connection: *mut objc2_virtualization::VZVirtioSocketConnection,
                          error: *mut objc2_foundation::NSError| {
                        let result = if !error.is_null() {
                            let err = unsafe { &*error };
                            Err(VzError::VsockFailed {
                                port,
                                reason: err.localizedDescription().to_string(),
                            })
                        } else if connection.is_null() {
                            Err(VzError::VsockFailed {
                                port,
                                reason: "connection returned null".into(),
                            })
                        } else {
                            unsafe { Retained::retain(connection) }
                                .map(SendableConnection)
                                .ok_or_else(|| VzError::VsockFailed {
                                    port,
                                    reason: "failed to retain connection".into(),
                                })
                        };
                        if let Some(tx) = tx.take() {
                            let _ = tx.send(result);
                        }
                    },
                );

                // SAFETY: connectToPort_completionHandler must be called on the VM's queue.
                unsafe { virtio_device.connectToPort_completionHandler(port, &block) };
            })
            .await?;

        let conn = rx.await.map_err(|_| VzError::VsockFailed {
            port,
            reason: "connect completion handler was never called".into(),
        })??;

        // Create VsockStream on the tokio thread where AsyncFd can register with the reactor.
        VsockStream::from_connection(conn.0)
    }

    /// Listen for incoming vsock connections from the guest.
    ///
    /// Returns a `VsockListener` that yields new connections via `accept()`.
    /// Requires vsock to be enabled in the VM configuration.
    pub async fn vsock_listen(&self, port: u32) -> Result<VsockListener, VzError> {
        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                // Get the first socket device from the VM
                let socket_devices = unsafe { handle.vm.socketDevices() };
                if socket_devices.is_empty() {
                    return Err(VzError::VsockFailed {
                        port,
                        reason: "no vsock device configured on this VM".into(),
                    });
                }

                let device_retained = socket_devices.to_vec().into_iter().next();
                let Some(device) = device_retained else {
                    return Err(VzError::VsockFailed {
                        port,
                        reason: "failed to get vsock device".into(),
                    });
                };
                let virtio_device =
                    Retained::downcast::<objc2_virtualization::VZVirtioSocketDevice>(device)
                        .map_err(|_| VzError::VsockFailed {
                            port,
                            reason: "socket device is not a VirtioSocketDevice".into(),
                        })?;

                VsockListener::new(&virtio_device, port)
            })
            .await?
    }

    /// Read the current target memory size in bytes.
    ///
    /// On a VM with the memory balloon enabled, this is the target size most
    /// recently written via [`set_target_memory_size`](Self::set_target_memory_size).
    /// Initially equals the VM's configured `memory_bytes`.
    ///
    /// Returns [`VzError::InvalidConfig`] if the VM was built without a memory
    /// balloon (`VmConfigBuilder::memory_balloon(false)`).
    pub async fn target_memory_size(&self) -> Result<u64, VzError> {
        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                // SAFETY: memoryBalloonDevices is a property accessor on VZVirtualMachine.
                let devices = unsafe { handle.vm.memoryBalloonDevices() };
                let device_retained = devices.to_vec().into_iter().next();
                let Some(device) = device_retained else {
                    return Err(VzError::InvalidConfig(
                        "memory balloon not enabled on this VM".into(),
                    ));
                };
                let traditional = Retained::downcast::<
                    objc2_virtualization::VZVirtioTraditionalMemoryBalloonDevice,
                >(device)
                .map_err(|_| {
                    VzError::InvalidConfig(
                        "memory balloon is not a VZVirtioTraditionalMemoryBalloonDevice".into(),
                    )
                })?;
                // SAFETY: targetVirtualMachineMemorySize is a property getter; always safe to read.
                let bytes = unsafe { traditional.targetVirtualMachineMemorySize() };
                Ok(bytes)
            })
            .await?
    }

    /// Ask the guest to balloon down (or back up) to `bytes` of available memory.
    ///
    /// Apple rounds the value down to a 1 MB boundary and clamps it to
    /// `[VZVirtualMachineConfiguration.minimumAllowedMemorySize, memory_bytes]`.
    /// The actual in-guest change is asynchronous — this method only writes
    /// the target. The guest's balloon driver eventually responds and pages
    /// move between guest and host.
    ///
    /// Returns [`VzError::InvalidConfig`] if the VM was built without a memory
    /// balloon (`VmConfigBuilder::memory_balloon(false)`).
    pub async fn set_target_memory_size(&self, bytes: u64) -> Result<(), VzError> {
        let handle = Arc::clone(&self.handle);
        self.queue
            .dispatch(move || {
                // SAFETY: memoryBalloonDevices is a property accessor on VZVirtualMachine.
                let devices = unsafe { handle.vm.memoryBalloonDevices() };
                let device_retained = devices.to_vec().into_iter().next();
                let Some(device) = device_retained else {
                    return Err(VzError::InvalidConfig(
                        "memory balloon not enabled on this VM".into(),
                    ));
                };
                let traditional = Retained::downcast::<
                    objc2_virtualization::VZVirtioTraditionalMemoryBalloonDevice,
                >(device)
                .map_err(|_| {
                    VzError::InvalidConfig(
                        "memory balloon is not a VZVirtioTraditionalMemoryBalloonDevice".into(),
                    )
                })?;
                // SAFETY: setTargetVirtualMachineMemorySize is a property setter; always safe.
                unsafe { traditional.setTargetVirtualMachineMemorySize(bytes) };
                Ok(())
            })
            .await?
    }

    /// Get the current VM state.
    pub fn state(&self) -> VmState {
        self.state_rx.borrow().clone()
    }

    /// Get a watch receiver for state changes.
    ///
    /// The receiver yields the new `VmState` every time it changes.
    /// Use `changed().await` to wait for the next transition.
    pub fn state_stream(&self) -> watch::Receiver<VmState> {
        self.state_rx.clone()
    }

    /// Attach a `VZVirtualMachineView` to this VM.
    ///
    /// Sets the view's `virtualMachine` property so it renders this VM's
    /// framebuffer. Must be called from the main thread (AppKit requirement).
    ///
    /// # Safety
    ///
    /// The caller must ensure this is called from the main thread.
    pub unsafe fn attach_view(&self, view: &objc2_virtualization::VZVirtualMachineView) {
        unsafe { view.setVirtualMachine(Some(&self.handle.vm)) };
    }
}

impl std::fmt::Debug for Vm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vm")
            .field("state", &self.state())
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{Vm, VmState, vz_state_to_vm_state};
    use objc2_virtualization::VZVirtualMachineState;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Apple's `VZVirtualMachineState` enum values must each translate
    /// to a distinct `VmState`. Unit-level guard so a future SDK bump
    /// that adds a variant doesn't silently collapse it to a phantom
    /// Stopped — pre-fix that exact silent collapse is what caused
    /// VRT-yl5l (boon prep VMs that "succeeded" with empty disks).
    #[test]
    fn vz_state_translation_covers_every_apple_variant() {
        let cases = [
            (VZVirtualMachineState::Stopped, VmState::Stopped),
            (VZVirtualMachineState::Starting, VmState::Starting),
            (VZVirtualMachineState::Running, VmState::Running),
            (VZVirtualMachineState::Pausing, VmState::Pausing),
            (VZVirtualMachineState::Paused, VmState::Paused),
            (VZVirtualMachineState::Resuming, VmState::Resuming),
            (VZVirtualMachineState::Stopping, VmState::Stopping),
            (VZVirtualMachineState::Saving, VmState::Saving),
            (VZVirtualMachineState::Restoring, VmState::Restoring),
        ];
        for (apple, ours) in cases {
            assert_eq!(
                vz_state_to_vm_state(apple),
                ours,
                "VZVirtualMachineState({}) should translate to {:?}",
                apple.0,
                ours,
            );
        }
        // VZVirtualMachineState::Error is a placeholder — the
        // descriptive NSError comes via the delegate's didStopWithError.
        assert!(matches!(
            vz_state_to_vm_state(VZVirtualMachineState::Error),
            VmState::Error(s) if s.is_empty(),
        ));
    }

    /// Forward-compat: an unknown `VZVirtualMachineState` raw value
    /// (e.g. an enum variant Apple adds in a future macOS SDK) lands
    /// as a tagged `VmState::Error` so it shows up in logs instead of
    /// being silently coerced to Stopped.
    #[test]
    fn unknown_vz_state_becomes_tagged_error() {
        let unknown = VZVirtualMachineState(9999);
        match vz_state_to_vm_state(unknown) {
            VmState::Error(msg) => {
                assert!(msg.contains("9999"), "error msg must echo the raw value: {msg}");
                assert!(msg.contains("unknown"), "error msg must be tagged: {msg}");
            }
            other => panic!("unknown raw state should become Error, got {other:?}"),
        }
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        base.push(format!(
            "vz-vm-checkpoint-test-{name}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    #[tokio::test]
    async fn fork_checkpoint_copies_artifact() {
        let temp = unique_temp_dir("fork-copy");
        let source = temp.join("source.state");
        let destination = temp.join("fork").join("destination.state");
        std::fs::write(&source, b"checkpoint-bytes").unwrap();

        Vm::fork_checkpoint(&source, &destination).await.unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), b"checkpoint-bytes");

        std::fs::remove_dir_all(temp).unwrap();
    }
}
