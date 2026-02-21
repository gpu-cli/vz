# 03 — Vsock gRPC Proxy

## Depends On

- 02 (guest BuildKit service — buildkitd listening on TCP in guest)

## Problem

The host-side build client needs to talk to buildkitd running inside the guest VM. BuildKit uses gRPC (HTTP/2). We need to bridge the guest's TCP socket to the host via vsock.

## Design

### Proxy Architecture

```
Host                              Guest
buildkit-client                   buildkitd
  │                                 │
  └── Unix socket ◄── vsock ──► TCP :8372
      /tmp/vz-buildkit.sock    port 7425
```

The vsock proxy is a bidirectional byte stream — it doesn't need to understand gRPC. Raw TCP bytes flow through.

### Using Existing Port Forwarding

The guest agent already supports `PortForward` requests (used for container port mapping). We reuse this:

```rust
// Open vsock stream to guest port 8372
let stream = open_port_forward_stream(&vm, 8372, "tcp").await?;
```

This gives us a `VsockStream` connected to `buildkitd:8372` inside the guest.

### Host-Side Socket

Expose the proxy as a Unix domain socket on the host so standard BuildKit clients can connect:

```rust
let listener = UnixListener::bind("/tmp/vz-buildkit.sock")?;
loop {
    let (client, _) = listener.accept().await?;
    let vsock = open_port_forward_stream(&vm, 8372, "tcp").await?;
    tokio::spawn(proxy_bidirectional(client, vsock));
}
```

Or, for our Rust client, skip the Unix socket entirely and connect the tonic gRPC channel directly over the vsock stream.

### Direct tonic Channel (Preferred)

Since we're building a Rust client, we can create a tonic `Channel` backed by the vsock stream directly:

```rust
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

let channel = Endpoint::try_from("http://[::]:50051")?  // URI ignored
    .connect_with_connector(service_fn(move |_: Uri| {
        let vm = vm.clone();
        async move {
            open_port_forward_stream(&vm, 8372, "tcp").await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        }
    }))
    .await?;
```

This eliminates the Unix socket intermediary entirely. The gRPC client talks directly over vsock.

### Implementation

New module: `vz-oci/src/buildkit/proxy.rs`

Two modes:
1. **Direct channel** — for our Rust client (Phase 4). Returns `tonic::Channel`.
2. **Unix socket proxy** — for external tools like `buildctl`. Optional, lower priority.

## Done When

1. `create_buildkit_channel(vm) -> tonic::Channel` returns a working gRPC channel over vsock
2. gRPC calls (e.g., `ListWorkers`, `Info`) succeed through the channel
3. Bidirectional streaming works (needed for `Session` and `Status` RPCs)
4. Optional: Unix socket proxy allows external `buildctl --addr unix:///tmp/vz-buildkit.sock`
5. Integration test: boot BuildKit VM, create channel, call `Info` RPC
