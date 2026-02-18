# 08 — Testing Strategy

## Challenge

Testing a virtualization library is hard because:

- VM tests need macOS + Apple Silicon (self-hosted runner required)
- VM operations are slow (boot: 30-60s, operations: seconds)
- Tests need a golden image to exist
- 2-VM limit constrains parallel test execution

## Testing Layers

### Layer 1: Unit Tests (no VM needed)

- Config validation (`VmConfigBuilder`)
- Error types
- Wire protocol serialization/deserialization (`Request`/`Response` enums, including handshake)
- State machine transitions (`VmState`)
- Channel framing (length-prefixed JSON encode/decode, base64 round-trip)

Run with: `cargo nextest run --workspace`

These should be the majority of tests and run fast.

### Layer 2: Integration Tests (need macOS, no VM)

- ObjC bridging (`NSString` conversions, `NSURL`, `NSError`)
- Dispatch queue creation
- `VZVirtualMachineConfiguration` validation
- Feature detection (`isSupported`, etc.)

Run with: `cargo nextest run --workspace` on macOS (skip on other platforms with `#[cfg(target_os = "macos")]`)

### Layer 3: VM Tests (need macOS + golden image)

- Full VM lifecycle: create, start, stop
- VirtioFS mount verification
- vsock connection
- Save/restore state
- Guest agent communication (including handshake negotiation)
- `SandboxSession` exec (with timeout, as user, as root)
- Session isolation (RestoreOnAcquire produces clean state)

Run with: `cargo nextest run --workspace --features vm-tests`

Behind a feature flag because they need:

- A golden image at a known path
- Minutes to run
- Exclusive access to the VM (can't parallelize)

### Layer 4: End-to-End Tests (CLI)

- `vz init` (with a local IPSW, skip download)
- `vz run` + `vz exec` + `vz stop` cycle
- `vz save` + `vz restore` cycle
- `vz cache list` / `vz cache clean`
- `vz cleanup` (orphaned VM detection)
- Error cases (missing image, invalid config, insufficient disk space)

Run with: shell scripts or `cargo nextest run -p vz-cli --features e2e-tests`

## CI Strategy

- **GitHub Actions (Linux)**: Run Layer 1 only (unit tests, cross-platform code compiles)
- **Self-hosted macOS ARM64 runner**: Run Layers 1-3 (requires Apple Silicon + golden image)
- **Manual**: Layer 4 (E2E) before releases

## Test Fixtures

`tests/fixtures/` directory with:

- Sample IPSW metadata (mock, not actual IPSW)
- Sample config files
- Wire protocol test vectors (JSON, including handshake and base64-encoded data)
- Entitlements plist for signing verification

## Mocking

- Trait-based abstraction for VM operations where possible
- `#[cfg(test)]` mock implementations
- For integration tests, use real ObjC objects but with minimal configurations

## Performance Benchmarks

- VM boot time (cold vs restore)
- VirtioFS throughput (read/write)
- vsock latency and throughput
- Guest agent command execution overhead

Use `criterion` crate, behind `--features bench` flag.

## Test Organization

```
crates/vz/tests/
├── config_test.rs       # Layer 1: config validation
├── protocol_test.rs     # Layer 1: wire protocol (including base64, handshake)
├── state_test.rs        # Layer 1: state machine
├── bridge_test.rs       # Layer 2: ObjC bridging
├── vm_lifecycle.rs      # Layer 3: full VM tests
└── vsock_test.rs        # Layer 3: vsock communication

crates/vz-sandbox/tests/
├── pool_test.rs         # Layer 1: pool logic
├── channel_test.rs      # Layer 1: channel framing
├── error_test.rs        # Layer 1: SandboxError variants
└── session_test.rs      # Layer 3: full session lifecycle

crates/vz-cli/tests/
└── cli_e2e.rs           # Layer 4: end-to-end CLI tests
```
