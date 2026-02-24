# Runtime Primitive Conformance Matrix

This matrix is the authoritative, published coverage map for Runtime V2 primitive behavior.

Source: `crates/vz-runtime-contract/src/lib.rs` (`PRIMITIVE_CONFORMANCE_MATRIX`).
The matrix is consumed by transport/manager parity tests in:

- `crates/vz-runtime-contract/src/lib.rs`
- `crates/vz-linux/src/grpc_client.rs`
- `crates/vz-api/src/lib.rs`
- `crates/vz-cli/src/commands/stack.rs`

## Matrix

| Runtime Operation | OpenAPI Path | Surface | Manager | gRPC Metadata | CLI |
| --- | --- | --- | --- | --- | --- |
| `CreateSandbox` | `/v1/sandboxes` | `sandboxes` | no | no | no |
| `GetSandbox` | `/v1/sandboxes` | `sandboxes` | no | no | no |
| `TerminateSandbox` | `/v1/sandboxes` | `sandboxes` | no | no | no |
| `OpenLease` | `/v1/leases` | `leases` | no | no | no |
| `HeartbeatLease` | `/v1/leases` | `leases` | no | no | no |
| `CloseLease` | `/v1/leases` | `leases` | no | no | no |
| `ResolveImage` | `/v1/images` | `images` | no | no | no |
| `PullImage` | `/v1/images` | `images` | yes | no | yes |
| `StartBuild` | `/v1/builds` | `builds` | no | no | no |
| `GetBuild` | `/v1/builds` | `builds` | no | no | no |
| `StreamBuildEvents` | `/v1/builds` | `builds` | no | no | no |
| `CancelBuild` | `/v1/builds` | `builds` | no | no | no |
| `CreateContainer` | `/v1/containers` | `containers` | yes | yes | yes |
| `StartContainer` | `/v1/containers` | `containers` | no | yes | no |
| `StopContainer` | `/v1/containers` | `containers` | yes | yes | yes |
| `RemoveContainer` | `/v1/containers` | `containers` | yes | yes | yes |
| `GetContainerLogs` | `/v1/containers` | `containers` | yes | no | yes |
| `ExecContainer` | `/v1/executions` | `executions` | yes | yes | yes |
| `WriteExecStdin` | `/v1/executions` | `executions` | no | no | no |
| `SignalExec` | `/v1/executions` | `executions` | no | no | no |
| `ResizeExecPty` | `/v1/executions` | `executions` | no | no | no |
| `CancelExec` | `/v1/executions` | `executions` | no | no | no |
| `CreateCheckpoint` | `/v1/checkpoints` | `checkpoints` | no | no | no |
| `RestoreCheckpoint` | `/v1/checkpoints` | `checkpoints` | no | no | no |
| `ForkCheckpoint` | `/v1/checkpoints` | `checkpoints` | no | no | no |
| `CreateVolume` | *(not claimed)* | *(n/a)* | no | no | no |
| `AttachVolume` | *(not claimed)* | *(n/a)* | no | no | no |
| `DetachVolume` | *(not claimed)* | *(n/a)* | no | no | no |
| `CreateNetworkDomain` | *(not claimed)* | *(n/a)* | no | yes | no |
| `PublishPort` | *(not claimed)* | *(n/a)* | no | no | no |
| `ConnectContainer` | *(not claimed)* | *(n/a)* | no | no | no |
| `ListEvents` | `/v1/events/{stack_name}` | `events` | no | no | no |
| `GetReceipt` | `/v1/receipts` | `receipts` | no | no | no |
| `GetCapabilities` | `/v1/capabilities` | `capabilities` | no | no | no |
