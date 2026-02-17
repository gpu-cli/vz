# Vsock Communication & Wire Format

## What is Vsock

Vsock (Virtual Socket, `AF_VSOCK`) is a host-guest communication mechanism that provides connection-oriented, bidirectional byte streams between a hypervisor host and its guest VMs. It uses a simple addressing scheme: each endpoint is identified by a Context ID (CID) and a port number.

- **Host CID**: 2 (always `VMADDR_CID_HOST`)
- **Guest CID**: 3 (or assigned by the hypervisor; Apple assigns CID 3 for single-guest setups)
- **Port range**: 0-4294967295 (u32)

Vsock requires no network configuration — no IP addresses, no DHCP, no routing tables. It works immediately when the VM starts, making it ideal for bootstrapping communication before any network stack is configured.

## Why Vsock Over Alternatives

| Property | vsock | SSH | virtio-serial | TCP/IP network |
|----------|-------|-----|---------------|----------------|
| Network config required | No | Yes (IP, sshd) | No | Yes (IP, DHCP) |
| Port multiplexing | Yes (u32 port space) | Yes (channels) | No (single stream) | Yes |
| Connection semantics | Yes (connect/accept) | Yes | No (raw byte pipe) | Yes |
| Standard socket API | Yes (AF_VSOCK) | Custom (libssh) | No (char device) | Yes |
| Authentication overhead | None | Key exchange, handshake | None | None |
| Latency | ~microseconds | ~milliseconds (crypto) | ~microseconds | ~milliseconds |
| macOS guest support | Yes | Yes | No | Yes |
| Stale data issues | No (connection-oriented) | No | Yes (buffer residue) | No |
| Setup complexity | Zero | High (keys, config, daemon) | Medium (device config) | Medium (networking) |

### Why not SSH

SSH requires an sshd daemon running in the guest, key management (generating, distributing, rotating keys), and a configured network stack. It adds ~1-3ms of latency per operation due to encryption/decryption. For a sandbox where the host fully controls the guest, SSH's authentication and encryption are pure overhead — there is no untrusted party to defend against.

### Why not virtio-serial

virtio-serial provides a raw byte pipe without connection semantics. If the host writes data and the guest is not reading, data accumulates in a buffer and becomes stale. There is no connect/disconnect lifecycle. Critically, **virtio-serial is not supported for macOS guests** in Apple's Virtualization.framework, making it a non-option.

### Why not TCP/IP networking

TCP/IP requires configuring a virtual network interface, DHCP or static IP assignment, and firewall rules. It exposes the guest to potential network-based attacks. For host-to-guest communication, vsock is strictly simpler and faster.

## Apple's Implementation

Apple's Virtualization.framework provides vsock through:

| Type | Role |
|------|------|
| `VZVirtioSocketDeviceConfiguration` | Added to VM config to enable vsock |
| `VZVirtioSocketDevice` | Runtime device handle, obtained from running VM |
| `VZVirtioSocketConnection` | A single connection (wraps a file descriptor pair) |
| `VZVirtioSocketListener` | Host-side listener for incoming guest connections |

### Host Connects to Guest

The primary pattern: host initiates a connection to the guest agent.

```swift
// Get the vsock device from the running VM
let socketDevice = vm.socketDevices.first!

// Connect to port 7424 on the guest
socketDevice.connect(toPort: 7424) { result in
    switch result {
    case .success(let connection):
        // connection.fileDescriptor — raw fd for read/write
        // connection.sourcePort — ephemeral port assigned on host side
    case .failure(let error):
        // Guest not listening, port unreachable, etc.
    }
}
```

### Guest Listens

The guest uses standard `AF_VSOCK` sockets:

```rust
use libc::{AF_VSOCK, VMADDR_CID_ANY, sockaddr_vm};

let fd = socket(AF_VSOCK, SOCK_STREAM, 0);
let addr = sockaddr_vm {
    svm_family: AF_VSOCK as u16,
    svm_cid: VMADDR_CID_ANY,    // accept from any CID (i.e., the host)
    svm_port: 7424,
    ..zeroed()
};
bind(fd, &addr);
listen(fd, 1);
let conn = accept(fd);          // blocks until host connects
```

## Wire Format: Length-Prefixed JSON

Every message on the vsock connection is framed as:

```
+-------------------+-------------------+
| length (4 bytes)  | JSON payload      |
| little-endian u32 | (length bytes)    |
+-------------------+-------------------+
```

- **length**: u32 in little-endian byte order, specifying the number of bytes in the JSON payload (not including the 4-byte length prefix itself).
- **JSON payload**: UTF-8 encoded JSON, exactly `length` bytes.

### Reading a frame

```rust
// Read the 4-byte length prefix
let mut len_buf = [0u8; 4];
stream.read_exact(&mut len_buf).await?;
let len = u32::from_le_bytes(len_buf) as usize;

// Read the JSON payload
let mut payload = vec![0u8; len];
stream.read_exact(&mut payload).await?;

// Deserialize
let msg: Message = serde_json::from_slice(&payload)?;
```

### Writing a frame

```rust
let json = serde_json::to_vec(&msg)?;
let len = (json.len() as u32).to_le_bytes();
stream.write_all(&len).await?;
stream.write_all(&json).await?;
```

### Maximum frame size

Enforce a maximum frame size of **16 MiB** (16,777,216 bytes). Any frame with a length prefix exceeding this limit is rejected and the connection is closed. This prevents a malformed or malicious message from causing unbounded memory allocation.

## Message Protocol

### Request (Host to Guest)

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Request {
    Exec {
        id: u64,
        command: String,
        args: Vec<String>,
        working_dir: Option<String>,
        env: Vec<(String, String)>,
    },
    StdinWrite {
        id: u64,
        exec_id: u64,
        data: Vec<u8>,
    },
    StdinClose {
        exec_id: u64,
    },
    Signal {
        exec_id: u64,
        signal: i32,
    },
    SystemInfo {
        id: u64,
    },
    Ping {
        id: u64,
    },
}
```

### Response (Guest to Host)

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum Response {
    Stdout {
        exec_id: u64,
        data: Vec<u8>,
    },
    Stderr {
        exec_id: u64,
        data: Vec<u8>,
    },
    ExitCode {
        exec_id: u64,
        code: i32,
    },
    ExecError {
        id: u64,
        error: String,
    },
    SystemInfoResult {
        id: u64,
        cpu_count: u32,
        memory_bytes: u64,
        disk_free_bytes: u64,
        os_version: String,
    },
    Pong {
        id: u64,
    },
    Error {
        id: u64,
        error: String,
    },
    Ok {
        id: u64,
    },
}
```

### Field Semantics

- **id**: unique per-request identifier, generated by the host. Used to correlate a response back to its originating request. Monotonically increasing.
- **exec_id**: assigned by the host in the `Exec` request. Used to correlate streaming `Stdout`/`Stderr`/`ExitCode` responses to the correct process. This is the same as the `id` field in the originating `Exec` request.
- **data** (in `Stdout`/`Stderr`/`StdinWrite`): raw bytes, encoded as a JSON array of numbers or base64 (TBD — JSON array is simpler, base64 is more compact).

## Streaming Design

The protocol is **push-based**: the guest agent sends `Stdout` and `Stderr` frames as soon as data is available from the child process, without waiting for the host to poll or request more data.

### Example Flow: `cargo build`

```
Host → Guest:  { "type": "Exec", "id": 1, "command": "cargo", "args": ["build"], "working_dir": "/mnt/workspace/my-project", "env": [] }

Guest → Host:  { "type": "Stdout", "exec_id": 1, "data": "   Compiling serde v1.0.197" }
Guest → Host:  { "type": "Stdout", "exec_id": 1, "data": "   Compiling tokio v1.36.0" }
Guest → Host:  { "type": "Stderr", "exec_id": 1, "data": "warning: unused variable `x`" }
Guest → Host:  { "type": "Stdout", "exec_id": 1, "data": "    Finished dev [unoptimized + debuginfo] target(s)" }
Guest → Host:  { "type": "ExitCode", "exec_id": 1, "code": 0 }
```

### Multiplexing

The `id` and `exec_id` fields enable concurrent commands on a single vsock connection:

```
Host → Guest:  Exec { id: 1, command: "cargo", args: ["build"] }
Host → Guest:  Exec { id: 2, command: "cargo", args: ["clippy"] }

Guest → Host:  Stdout { exec_id: 1, data: "   Compiling..." }
Guest → Host:  Stderr { exec_id: 2, data: "warning: ..." }
Guest → Host:  Stdout { exec_id: 1, data: "    Finished..." }
Guest → Host:  ExitCode { exec_id: 1, code: 0 }
Guest → Host:  ExitCode { exec_id: 2, code: 0 }
```

Responses from different processes can interleave freely. The host demultiplexes by `exec_id`.

### Backpressure

If the host cannot consume frames fast enough, TCP-like backpressure applies naturally through the vsock transport: the guest's `write()` calls will block when the kernel buffer fills up, which in turn applies backpressure to the child process's stdout/stderr pipes.

## Why Not gRPC / Protobuf / ttrpc

### gRPC

gRPC requires HTTP/2 framing and protobuf serialization. For a single vsock channel between a host and guest that we fully control, HTTP/2's multiplexing, header compression, and flow control are unnecessary overhead. gRPC also pulls in heavy dependencies (tonic, prost, hyper) and requires a `.proto` build step.

### Protobuf

Protobuf alone (without gRPC) is a reasonable choice, but binary messages are painful to debug. When something goes wrong at the protocol level, being able to `hexdump` a vsock frame and see readable JSON is invaluable. The performance difference is negligible for our message sizes (commands and output lines, not bulk data transfer).

### ttrpc

ttrpc is the "tiny gRPC" used by Kata Containers. The Rust implementation (`ttrpc-rust`) is tightly coupled to containerd's ecosystem and assumes Linux. It adds protobuf codegen requirements without the ecosystem benefits of gRPC.

### Length-Prefixed JSON

- **Simple**: ~20 lines of framing code, no codegen, no build plugins.
- **Debuggable**: frames are human-readable; invaluable during development.
- **Native serde**: Rust's `serde_json` handles serialization directly from the protocol types.
- **Zero codegen**: no `.proto` files, no build.rs, no generated code.
- **Good enough performance**: for command execution and output streaming, JSON serialization is not the bottleneck. The child process I/O and vsock transport dominate latency.
