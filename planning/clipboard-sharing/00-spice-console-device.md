# Phase 0: Add SPICE Console Device to VM Config

Add `VZSpiceAgentPortAttachment` on a VirtIO console device so the host side of clipboard sharing is wired up.

## Depends On

Nothing — this is pure host-side configuration.

## Step 1: Add objc2-virtualization feature flags

**File**: `crates/Cargo.toml`

Add these features to the `objc2-virtualization` dependency:

```toml
# SPICE clipboard sharing (console device + agent attachment)
"VZSerialPortAttachment",
"VZSpiceAgentPortAttachment",
"VZConsoleDeviceConfiguration",
"VZConsolePortConfiguration",
"VZVirtioConsoleDeviceConfiguration",
"VZVirtioConsolePortConfiguration",
"VZVirtioConsolePortConfigurationArray",
```

These are all parent + child class features needed for the objc2 type hierarchy.

## Step 2: Add clipboard option to VmConfig

**File**: `crates/vz/src/config.rs`

Add a `clipboard` field to `VmConfigBuilder` and `VmConfig`:

```rust
// In VmConfigBuilder:
clipboard: bool,  // default: false

// Builder method:
pub fn enable_clipboard(mut self) -> Self {
    self.clipboard = true;
    self
}
```

Carry through to `VmConfig` the same way `vsock: bool` is handled.

## Step 3: Wire console device into build_objc_config

**File**: `crates/vz/src/bridge.rs`

After the vsock section (around line 390), add:

```rust
// SPICE clipboard sharing via VirtIO console
if config.clipboard {
    let spice_attachment = unsafe { VZSpiceAgentPortAttachment::new() };
    unsafe { spice_attachment.setSharesClipboard(true) };

    let console_port = unsafe { VZVirtioConsolePortConfiguration::new() };
    unsafe {
        console_port.setName(Some(&NSString::from_str(
            &VZSpiceAgentPortAttachment::spiceAgentPortName().to_string(),
        )));
        console_port.setAttachment(Some(&spice_attachment));
        console_port.setIsConsole(false);
    }

    let console_device = unsafe { VZVirtioConsoleDeviceConfiguration::new() };
    unsafe { console_device.ports().setObject_atIndex(&console_port, 0) };

    let console_devices = NSArray::from_retained_slice(&[
        Retained::into_super(console_device),
    ]);
    unsafe { vz_config.setConsoleDevices(&console_devices) };
}
```

**Important**: The `spiceAgentPortName` is a class method returning `NSString`. Use it directly rather than hardcoding `"com.redhat.spice.0"`.

**Note on the ports array API**: `VZVirtioConsolePortConfigurationArray` uses subscript access. In objc2, this may be `setObject:atIndex:` or a similar method. Need to check the actual objc2-virtualization API — it might be `setObject_atIndexedSubscript` or the ports array might have a different setter pattern. Check the generated bindings.

## Step 4: Enable clipboard in CLI

**File**: `crates/vz-cli/src/commands/run.rs`

In `setup()`, when building the VM config, add `.enable_clipboard()` when not headless (GUI mode). Can also enable for headless since it's harmless — the console device exists but without a guest agent connecting, it's a no-op.

```rust
// Always enable clipboard — harmless without guest agent
builder = builder.enable_clipboard();
```

## Step 5: Re-export from lib.rs

**File**: `crates/vz/src/lib.rs`

No new public types needed — `enable_clipboard()` is just a builder method.

## Validation

1. `cargo clippy --workspace -- -D warnings` — clean
2. `cargo nextest run --workspace` — all tests pass
3. `vz run --image base.img --name test` — starts without error
4. VM save/restore still works with the console device added
5. `diskutil list` or console output confirms no regression

## Notes

- The console device is lightweight — it's just a VirtIO serial port. No performance impact.
- Without a guest-side `spice-vdagent`, the port exists but nothing connects to it. No errors.
- `setCapturesSystemKeys(true)` does NOT interfere with SPICE clipboard — they operate on different layers (keyboard vs. protocol).
