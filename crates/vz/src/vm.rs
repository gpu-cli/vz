//! Virtual machine lifecycle management.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use objc2::AnyThread;
use objc2::rc::Retained;
use objc2_virtualization::VZVirtualMachine;
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

        // Set up the state channel. The sender will be moved into the dispatch
        // closure where it's used to create the VMDelegate.
        let (state_tx, state_rx) = watch::channel(VmState::Stopped);

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
                let delegate = VMDelegate::new(state_tx);

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
            state_rx,
            _config: config,
        })
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

        await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::StartFailed(msg),
            other => other,
        })
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

        await_completion(rx).await
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

        await_completion(rx).await
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

        await_completion(rx).await.map_err(|e| match e {
            VzError::FrameworkError(msg) => VzError::StopFailed(msg),
            other => other,
        })
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
    use super::Vm;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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
