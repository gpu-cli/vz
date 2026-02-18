//! GUI mode: native macOS window showing the VM's framebuffer.
//!
//! Uses `VZVirtualMachineView` (from Virtualization.framework) inside an
//! `NSWindow` managed by AppKit. The tokio runtime runs on a background
//! thread while the main thread drives the AppKit event loop.

// GUI code uses objc2 APIs that require unsafe for ObjC interop.

use std::sync::Arc;

use objc2::MainThreadMarker;
use objc2::MainThreadOnly;
use objc2::rc::Retained;
use objc2_app_kit::{
    NSApplication, NSApplicationActivationPolicy, NSBackingStoreType, NSWindow, NSWindowStyleMask,
};
use objc2_foundation::{NSPoint, NSRect, NSSize, NSString};
use objc2_virtualization::VZVirtualMachineView;
use tracing::{error, info};

use crate::commands::run::{self, RunArgs};

/// Run a VM with a GUI window on the main thread.
///
/// This takes over the main thread for AppKit. The tokio runtime and VM
/// control server run on background threads. When the window closes or
/// the user presses Ctrl+C, the VM is stopped and the process exits.
pub fn run_with_gui(args: RunArgs) -> anyhow::Result<()> {
    // We must be on the main thread for AppKit.
    let mtm = MainThreadMarker::new()
        .ok_or_else(|| anyhow::anyhow!("GUI mode must be called from the main thread"))?;

    // Set up NSApplication before creating any windows.
    let app = NSApplication::sharedApplication(mtm);
    app.setActivationPolicy(NSApplicationActivationPolicy::Regular);

    // Create a tokio runtime for async VM work.
    let rt = tokio::runtime::Runtime::new()?;

    // Create and start the VM on the tokio runtime (blocking the main thread
    // briefly). This is fine — the AppKit event loop hasn't started yet.
    let running = rt.block_on(run::setup(&args))?;
    let vm = running.vm.clone();
    let name = running.name.clone();

    // Create the VM window on the main thread.
    let _window = create_vm_window(mtm, &vm, &name);

    // Spawn the control server event loop on tokio (background thread).
    // When the app terminates, we do cleanup in the signal handler.
    let vm_for_cleanup = running.vm.clone();
    let name_for_cleanup = running.name.clone();
    rt.spawn(async move {
        // Wait for VM stop signal (from control socket, e.g. `vz stop`)
        run::wait_and_cleanup(running).await.ok();
    });

    // Register Ctrl+C handler that exits cleanly.
    let rt_handle = rt.handle().clone();
    ctrlc_handler(rt_handle, vm_for_cleanup, name_for_cleanup);

    // Show credentials (read password from sidecar file if available)
    let password = crate::provision::read_saved_password(&args.image)
        .unwrap_or_else(|| "(unknown)".to_string());
    println!("Login: dev / {password}");
    println!("Ctrl+C to stop");

    // Enter the AppKit event loop. This blocks forever.
    app.run();

    Ok(())
}

/// Create an NSWindow with a VZVirtualMachineView as its content view.
fn create_vm_window(mtm: MainThreadMarker, vm: &Arc<vz::Vm>, name: &str) -> Retained<NSWindow> {
    let content_rect = NSRect::new(
        NSPoint { x: 200.0, y: 200.0 },
        NSSize {
            width: 1280.0,
            height: 800.0,
        },
    );

    let style = NSWindowStyleMask::Titled
        | NSWindowStyleMask::Closable
        | NSWindowStyleMask::Resizable
        | NSWindowStyleMask::Miniaturizable;

    let window = unsafe {
        NSWindow::initWithContentRect_styleMask_backing_defer(
            NSWindow::alloc(mtm),
            content_rect,
            style,
            NSBackingStoreType(2), // NSBackingStoreBuffered = 2
            false,
        )
    };

    let title = NSString::from_str(&format!("vz — {name}"));
    window.setTitle(&title);

    // Create VZVirtualMachineView and attach the VM.
    let vm_view = unsafe { VZVirtualMachineView::new(mtm) };
    // SAFETY: we are on the main thread (guaranteed by MainThreadMarker).
    unsafe { vm.attach_view(&vm_view) };

    // Enable keyboard capture so the VM receives keystrokes.
    unsafe { vm_view.setCapturesSystemKeys(true) };

    // Set as content view and show.
    window.setContentView(Some(&vm_view));
    window.makeKeyAndOrderFront(None);
    window.center();

    // Activate the app so the window comes to front.
    #[allow(deprecated)]
    unsafe {
        app_activate(mtm);
    }

    window
}

/// Activate the application (bring to front).
#[allow(deprecated)]
unsafe fn app_activate(mtm: MainThreadMarker) {
    let app = NSApplication::sharedApplication(mtm);
    app.activateIgnoringOtherApps(true);
}

/// Set up a Ctrl+C handler that cleans up and exits.
fn ctrlc_handler(rt: tokio::runtime::Handle, vm: Arc<vz::Vm>, name: String) {
    // Use a raw signal handler since NSApplication.run() owns the main thread.
    std::thread::spawn(move || {
        // Block until SIGINT
        let (tx, rx) = std::sync::mpsc::channel();
        let _ = ctrlc::set_handler(move || {
            let _ = tx.send(());
        });

        // Wait for the signal
        if rx.recv().is_ok() {
            info!("received Ctrl+C, stopping VM");

            // Clean up on the tokio runtime
            rt.block_on(async {
                if let Err(e) = vm.request_stop().await {
                    error!(error = %e, "graceful stop failed");
                    let _ = vm.stop().await;
                }
            });

            // Unregister
            if let Ok(mut registry) = crate::registry::Registry::load() {
                registry.remove(&name);
                let _ = registry.save();
            }
            let _ = std::fs::remove_file(crate::control::socket_path(&name));

            std::process::exit(0);
        }
    });
}
