# 05 — Base vz Changes Required for OCI Runtime

## Purpose

The OCI runtime (vz-linux, vz-oci) depends on capabilities in the base vz crate and protocol that may not yet exist when the base implementation completes. This document lists the changes needed in the base crates before or during OCI runtime implementation.

These should be applied after the base vz implementation is complete and before OCI runtime work begins.

## Cargo.toml Feature Additions

Add to `objc2-virtualization` features in `crates/Cargo.toml`:

```toml
# Linux boot (required for vz-linux)
"VZLinuxBootLoader",
"VZGenericPlatformConfiguration",

# Vsock listener (required for port forwarding)
"VZVirtioSocketListener",

# Optional: Rosetta for future x86_64 image support
# "VZLinuxRosettaDirectoryShare",
```

Add new workspace members:

```toml
members = [
    "vz",
    "vz-sandbox",
    "vz-cli",
    "vz-guest-agent",
    "vz-linux",     # NEW
    "vz-oci",       # NEW
]

[workspace.dependencies]
# ... existing ...
vz-linux = { path = "vz-linux" }
vz-oci = { path = "vz-oci" }
```

## Vestigial vz-sys Crate

`crates/vz-sys/` exists but is not a workspace member and is not used. It should be deleted to avoid confusion, as the planning docs explicitly state the project has 3 crates (not 4) and uses `objc2-virtualization` instead.

## HandshakeAck: Add `os` Field

The `HandshakeAck` struct needs an `os: String` field so the host can distinguish macOS from Linux guest agents:

```rust
struct HandshakeAck {
    protocol_version: u32,
    agent_version: String,
    os: String,              // "macos" or "linux" — NEW
    capabilities: Vec<String>,
}
```

This is referenced in `01-linux-vm.md` which states "the handshake reports `os_version` which the host can use if it cares."

## VmConfigBuilder: Additional Methods

The OCI runtime needs three builder methods not currently in the base API:

| Method | Signature | Purpose |
|--------|-----------|---------|
| `.shared_dirs(cfgs)` | `Vec<SharedDirConfig>` | Batch add VirtioFS mounts (convenience for programmatic mount lists) |
| `.memory_mb(n)` | `u64` | RAM in megabytes — Linux containers use 512 MB, which can't be expressed as integer GB |
| `.network(cfg)` | `NetworkConfig` | Set network from a `NetworkConfig` enum value directly |

These are convenience methods — the OCI runtime can work around their absence by looping `.shared_dir()` calls, using `.memory_bytes(n * 1024 * 1024)`, and matching on `NetworkConfig` to call `.network_nat()` / `.network_bridged()`. But the builder methods are cleaner.

## Protocol Extensions: PortForward

The port forwarding feature (03-container-lifecycle.md) requires two new protocol message types:

```rust
// Request
PortForward {
    id: u64,
    target_port: u16,
    protocol: String,  // "tcp" or "udp"
}

// Response
PortForwardReady {
    id: u64,
}
```

After `PortForwardReady`, the vsock connection becomes a raw bidirectional byte pipe (no more length-prefixed JSON framing). This must be sent on a **new** vsock connection, not the control connection on port 7424.

Add `"port_forward"` to the capabilities list. The host checks for this capability before attempting port forwarding.

## Channel: Stream Split for Concurrent Access

The `Channel<Req, Resp>` type needs interior mutability to support concurrent `send()` and `recv()` from different tasks. The current implementation uses a single `VsockStream` with `&self` methods, which requires mutable access.

Fix: Split the stream into read/write halves using `tokio::io::split()`, wrap each in `Mutex`:

```rust
pub struct Channel<Req, Resp> {
    reader: Mutex<OwnedReadHalf>,
    writer: Mutex<OwnedWriteHalf>,
    _phantom: PhantomData<(Req, Resp)>,
}
```

## Protocol Types: Shared Location

`Request`, `Response`, `ExecOutput`, `ExecStream`, `ExecEvent`, `Channel`, and `ResourceStats` are used by both `vz-sandbox` (macOS) and `vz-oci` (Linux). Neither crate should depend on the other.

Strategy: Move these types into a `vz::protocol` module within the `vz` crate. Both `vz-sandbox` and `vz-oci` already depend on `vz`, so both can import `vz::protocol::*`. The guest agent also depends on these types (compiled for the guest target).
