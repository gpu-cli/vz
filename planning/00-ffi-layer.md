# 00 — FFI Layer: objc2-virtualization Bindings

How vz talks to Virtualization.framework without a custom sys crate.

---

## Why No vz-sys Crate

The `objc2-virtualization` crate (v0.3.2, from the [madsmtm/objc2](https://github.com/madsmtm/objc2) project) already provides auto-generated bindings to **every** Virtualization.framework class. The bindings are generated from Xcode 16.4 SDK headers, so they cover the full public API surface including macOS 13, 14, and 15 additions.

Each class is gated behind its own Cargo feature flag. You enable exactly the classes you need:

- `VZVirtualMachine` — the VM itself
- `VZVirtualMachineConfiguration` — hardware config builder
- `VZMacOSBootLoader` — Apple Silicon boot
- `VZMacPlatformConfiguration` — Mac hardware model, machine identifier, auxiliary storage
- `VZMacOSRestoreImage` — IPSW fetching/loading
- `VZMacOSInstaller` — macOS installation into a VM
- `VZVirtioFileSystemDeviceConfiguration` — virtio-fs shared directories
- `VZVirtioSocketDeviceConfiguration` — vsock
- ... and so on

Because these bindings are complete and maintained upstream, there is no reason to write or maintain a `vz-sys` crate. The `vz` crate depends directly on `objc2-virtualization`.

---

## Required Dependencies

```toml
[dependencies]
objc2 = "0.6"
objc2-foundation = { version = "0.3", features = [
    "NSObject",
    "NSString",
    "NSError",
    "NSURL",
    "NSArray",
] }
objc2-virtualization = { version = "0.3", features = [
    # Core VM
    "VZVirtualMachine",
    "VZVirtualMachineConfiguration",

    # Boot and platform
    "VZMacOSBootLoader",
    "VZMacPlatformConfiguration",
    "VZMacOSRestoreImage",
    "VZMacOSInstaller",

    # Shared filesystem (virtio-fs)
    "VZVirtioFileSystemDeviceConfiguration",
    "VZSharedDirectory",
    "VZMultipleDirectoryShare",
    "VZSingleDirectoryShare",

    # Vsock
    "VZVirtioSocketDeviceConfiguration",
    "VZVirtioSocketDevice",
    "VZVirtioSocketConnection",

    # Disk
    "VZDiskImageStorageDeviceAttachment",
    "VZVirtioBlockDeviceConfiguration",

    # Network
    "VZNATNetworkDeviceAttachment",
    "VZVirtioNetworkDeviceConfiguration",

    # Display
    "VZMacGraphicsDeviceConfiguration",

    # Delegate, start options
    "VZVirtualMachineDelegate",
    "VZVirtualMachineStartOptions",
    "VZMacOSVirtualMachineStartOptions",

    # Block and dispatch interop
    "block2",
    "dispatch2",
] }
block2 = "0.6"
dispatch2 = { version = "0.3", features = ["alloc", "objc2"] }
```

Additional features are added as needed. The pattern is: if you use a class, enable its feature flag. Compilation will tell you immediately if you forgot one.

---

## Serial Dispatch Queue (CRITICAL)

**All `VZVirtualMachine` operations MUST execute on a dedicated serial dispatch queue.** This is not a suggestion — Virtualization.framework will crash or silently corrupt state if you call VM methods from arbitrary threads.

The Go binding library [Code-Hex/vz](https://github.com/Code-Hex/vz) enforces this by wrapping every single VM operation in `dispatch_sync` on a serial queue. Rust must do the same.

### Creating the queue and the VM

```rust
use dispatch2::DispatchQueue;
use objc2::rc::Retained;
use objc2_foundation::ns_string;
use objc2_virtualization::{VZVirtualMachine, VZVirtualMachineConfiguration};

let queue = DispatchQueue::new(
    Some(ns_string!("com.vz.vm-queue")),
    None, // None = serial (DISPATCH_QUEUE_SERIAL)
);

// The VM is created ON the queue, bound to it for its lifetime.
let vm: Retained<VZVirtualMachine> = unsafe {
    VZVirtualMachine::initWithConfiguration_queue(
        VZVirtualMachine::alloc(),
        &config,
        &queue,
    )
};
```

### Dispatching operations

Every method call on the VM (start, stop, pause, resume, state queries) must happen on this queue. The pattern is:

```rust
queue.exec_async(move || {
    // Safe to call VM methods here.
    unsafe { vm.startWithCompletionHandler(&block) };
});
```

To bridge the result back to async Rust, use a `tokio::sync::oneshot` channel (see next section).

---

## Async Bridging: ObjC Blocks to Tokio Futures

Most Virtualization.framework async operations use ObjC completion handler blocks. The bridge to Rust async is:

1. Create a `tokio::sync::oneshot` channel.
2. Wrap the sender in a `Cell<Option<Sender>>` so the `Fn` block can `.take()` it exactly once.
3. Pass an `RcBlock` as the completion handler.
4. Await the receiver.

### Why Cell + Option + take?

`RcBlock::new` requires `Fn`, not `FnOnce`. But a oneshot sender can only be used once. The `Cell<Option<...>>` pattern lets us move the sender out on first invocation and no-op on any subsequent call (which should never happen, but the type system does not know that).

### Pattern: startWithCompletionHandler

```rust
use std::cell::Cell;
use block2::RcBlock;
use objc2_foundation::NSError;
use objc2_virtualization::VZVirtualMachine;
use tokio::sync::oneshot;

fn start_async(vm: &VZVirtualMachine) -> oneshot::Receiver<Result<(), String>> {
    let (tx, rx) = oneshot::channel();
    let tx = Cell::new(Some(tx));

    let block = RcBlock::new(move |error: *mut NSError| {
        let result = if error.is_null() {
            Ok(())
        } else {
            Err(unsafe { &*error }.to_string())
        };
        if let Some(tx) = tx.take() {
            let _ = tx.send(result);
        }
    });

    unsafe { vm.startWithCompletionHandler(&block) };
    rx
}
```

This same pattern applies to:
- `pauseWithCompletionHandler:`
- `resumeWithCompletionHandler:`
- `stopWithCompletionHandler:`
- `saveMachineStateTo:completionHandler:` (macOS 14+)
- `restoreMachineStateFrom:completionHandler:` (macOS 14+)
- `VZMacOSRestoreImage::fetchLatestSupportedWithCompletionHandler:`

### Putting it together with the dispatch queue

```rust
async fn vm_start(vm: &VZVirtualMachine, queue: &DispatchQueue) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    let tx = Cell::new(Some(tx));

    let block = RcBlock::new(move |error: *mut NSError| {
        let result = if error.is_null() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("{}", unsafe { &*error }))
        };
        if let Some(tx) = tx.take() {
            let _ = tx.send(result);
        }
    });

    // Must dispatch onto the VM's serial queue
    let vm = vm.retain();
    queue.exec_async(move || {
        unsafe { vm.startWithCompletionHandler(&block) };
    });

    rx.await?
}
```

---

## Delegate Implementation with define_class!

`VZVirtualMachineDelegate` delivers state change callbacks: the guest stopped cleanly, or the VM stopped with an error. We implement this by defining an ObjC class in Rust using the `define_class!` macro.

### Complete delegate implementation

```rust
use std::cell::Cell;
use objc2::define_class;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2_foundation::{NSError, NSObject};
use objc2_virtualization::{VZVirtualMachine, VZVirtualMachineDelegate};
use tokio::sync::watch;

/// VM lifecycle states visible to the rest of the crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmState {
    Starting,
    Running,
    Pausing,
    Paused,
    Resuming,
    Stopping,
    Stopped,
    Error(String),
}

define_class!(
    // The class inherits from NSObject.
    #[unsafe(super = NSObject)]
    pub struct VMDelegate {
        /// Watch channel sender — wrapped in Cell because ObjC methods receive &self.
        state_tx: Cell<Option<watch::Sender<VmState>>>,
    }

    // Required: NSObjectProtocol conformance.
    unsafe impl NSObjectProtocol for VMDelegate {}

    // VZVirtualMachineDelegate protocol methods.
    unsafe impl VZVirtualMachineDelegate for VMDelegate {
        /// Called when the guest OS initiates a clean shutdown.
        #[unsafe(method(guestDidStopVirtualMachine:))]
        fn guest_did_stop(&self, _vm: &VZVirtualMachine) {
            if let Some(tx) = self.state_tx.take() {
                let _ = tx.send(VmState::Stopped);
                // Put it back so future events (if any) don't panic.
                self.state_tx.set(Some(tx));
            }
        }

        /// Called when the VM stops due to an error.
        #[unsafe(method(virtualMachine:didStopWithError:))]
        fn vm_did_stop_with_error(&self, _vm: &VZVirtualMachine, error: &NSError) {
            if let Some(tx) = self.state_tx.take() {
                let msg = error.to_string();
                let _ = tx.send(VmState::Error(msg));
                self.state_tx.set(Some(tx));
            }
        }
    }
);

impl VMDelegate {
    pub fn new(tx: watch::Sender<VmState>) -> Retained<Self> {
        let this = Self::alloc();
        // Initialize ivars.
        let this = unsafe { Self::init(this) };
        this.state_tx.set(Some(tx));
        this
    }
}
```

### Wiring the delegate to a VM

```rust
let (state_tx, state_rx) = watch::channel(VmState::Starting);
let delegate = VMDelegate::new(state_tx);

// Set the delegate — must happen on the VM's serial queue.
queue.exec_async(move || {
    unsafe { vm.setDelegate(Some(&delegate)) };
});

// Now state_rx will receive VmState updates from the delegate.
```

---

## Foundation Type Conversions

### NSString

```rust
use objc2_foundation::{ns_string, NSString};

// Compile-time literal (zero-cost, static lifetime):
let tag = ns_string!("com.vz.vm-queue");

// Runtime conversion from &str / String:
let dynamic = NSString::from_str("hello");

// Back to Rust String:
let rust_string: String = ns_string.to_string();
```

### NSURL

```rust
use objc2_foundation::NSURL;

// File path to NSURL:
let url = unsafe {
    NSURL::initFileURLWithPath(NSURL::alloc(), &NSString::from_str("/path/to/disk.img"))
};

// NSURL back to PathBuf:
let path = PathBuf::from(url.path().unwrap().to_string());
```

### NSError

```rust
// NSError implements Display, so:
let msg = format!("{}", error);           // localized description
let msg = error.localizedDescription();   // explicit
let code = error.code();                  // NSInteger error code
let domain = error.domain().to_string();  // error domain string
```

Map to anyhow:

```rust
fn ns_error_to_anyhow(err: &NSError) -> anyhow::Error {
    anyhow::anyhow!("Virtualization error ({}:{}): {}", err.domain(), err.code(), err)
}
```

### NSArray

```rust
use objc2_foundation::NSArray;
use objc2::rc::Retained;

// Create from a Vec of Retained objects:
let items: Vec<Retained<VZVirtioBlockDeviceConfiguration>> = vec![disk0, disk1];
let array = NSArray::from_retained_slice(&items);
```

---

## Memory Management

### Retained<T>

`Retained<T>` is the primary smart pointer for ObjC objects. It is analogous to `Arc` — it holds a strong reference and releases on drop.

```rust
// Alloc + init returns Retained<Self>:
let config: Retained<VZVirtualMachineConfiguration> = unsafe {
    VZVirtualMachineConfiguration::init(VZVirtualMachineConfiguration::alloc())
};

// Clone bumps the refcount:
let config2 = config.clone(); // config2: Retained<VZVirtualMachineConfiguration>

// .retain() on a reference also produces Retained:
let vm_retained = vm.retain();
```

### autoreleasepool

Temporary `NSString` references and other autorelease returns should be wrapped:

```rust
use objc2::rc::autoreleasepool;

autoreleasepool(|_pool| {
    let desc = error.localizedDescription();
    tracing::error!("VM error: {desc}");
    // desc is valid within this pool scope.
});
```

### Alloc/Init pattern

All Virtualization.framework objects follow alloc then init:

```rust
// Generic pattern:
let obj = unsafe { SomeClass::initWithFoo(SomeClass::alloc(), &arg) };
// Returns Retained<SomeClass>
```

The `alloc()` call returns `Allocated<Self>`, and the `init*` method consumes it and returns `Retained<Self>`.

---

## Thread Safety

### VZVirtualMachine is NOT thread-safe

Apple documents this explicitly. All interactions must happen on the serial dispatch queue the VM was created with. Violating this causes crashes, hangs, or silent corruption.

Rules:
1. Create the VM with `initWithConfiguration_queue`.
2. Every method call on that VM instance goes through `queue.exec_async` or `queue.exec_sync`.
3. Never share the VM reference across threads without dispatching.

### MainThreadMarker for AppKit APIs

Display-related APIs (`VZMacGraphicsDeviceConfiguration`, `VZMacGraphicsDisplay`) sometimes require the main thread. Use:

```rust
use dispatch2::MainThreadMarker;

let mtm = MainThreadMarker::new()
    .expect("must be called from the main thread");
```

For headless VMs (no display), this is not needed.

### MainThreadBound<T>

If you need to hold a `!Send` type (like a VZVirtualMachine reference) in a struct that crosses await points, wrap it:

```rust
use objc2::MainThreadBound;

struct VmHandle {
    vm: MainThreadBound<Retained<VZVirtualMachine>>,
}
```

Access requires proving you are on the correct thread.

### Bridging dispatch queue to tokio

The canonical pattern for bridging synchronous dispatch queue execution back to async tokio code:

```rust
async fn dispatch_and_await<T: Send + 'static>(
    queue: &DispatchQueue,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = oneshot::channel();
    queue.exec_async(move || {
        let result = f();
        let _ = tx.send(result);
    });
    rx.await.expect("dispatch queue dropped sender")
}
```

---

## macOS Version Gating

Virtualization.framework capabilities vary by macOS version:

| macOS Version | Key Additions |
|---------------|---------------|
| 13 (Ventura) | Base framework: VZVirtualMachine, Mac platform, boot loaders, virtio-fs, vsock, disk, network |
| 14 (Sonoma) | `saveMachineStateTo:completionHandler:`, `restoreMachineStateFrom:completionHandler:` (save/restore), clipboard sharing |
| 15 (Sequoia) | Additional device types, nested virtualization improvements |

### Feature flags

The `vz` crate uses cumulative feature flags:

```toml
[features]
default = ["macos-13"]
macos-13 = []          # Base support
macos-14 = ["macos-13"] # Adds save/restore
```

Code gated behind version flags:

```rust
#[cfg(feature = "macos-14")]
pub async fn save_state(&self, path: &Path) -> Result<()> {
    let url = nsurl_from_path(path);
    let rx = save_machine_state_async(&self.vm, &url);
    rx.await??;
    Ok(())
}
```

### Runtime checks

For cases where you want a single binary that adapts at runtime:

```rust
use objc2_foundation::NSProcessInfo;

fn macos_version() -> (usize, usize, usize) {
    let info = NSProcessInfo::processInfo();
    let version = info.operatingSystemVersion();
    (version.majorVersion as usize,
     version.minorVersion as usize,
     version.patchVersion as usize)
}

fn supports_save_restore() -> bool {
    let (major, minor, _) = macos_version();
    major > 14 || (major == 14 && minor >= 0)
}
```

Prefer compile-time feature flags for the common case. Use runtime checks only when distributing a universal binary that must degrade gracefully.

---

## Summary

The FFI stack is:

```
vz (safe Rust API)
  |
  +-- objc2-virtualization  (auto-generated bindings, feature-flagged per class)
  |     |
  |     +-- objc2            (ObjC runtime, messaging, class definition)
  |     +-- objc2-foundation (NSString, NSURL, NSError, NSArray)
  |     +-- block2           (ObjC block closures)
  |     +-- dispatch2        (GCD serial queues)
  |
  +-- tokio                  (async runtime, oneshot channels for bridging)
```

No custom C bindings. No bindgen. No build.rs linking hacks. The objc2 ecosystem handles everything, and vz builds a safe, async Rust API on top.
