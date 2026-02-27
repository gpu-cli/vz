# Runtime V2 Streaming Contract

This document defines the streaming behavior contract for `vz.runtime.v2`.

## Scope

Two stream classes exist:

- Mutation lifecycle streams: finite progress -> terminal completion or terminal gRPC error.
- Event/tail streams: potentially unbounded output streams that end on producer completion or client disconnect.

## Shared Invariants

- `request_id` must remain stable across all events in one stream.
- `sequence` must be strictly increasing for event types that include a `sequence` field.
- Ordering is FIFO per stream.

## Mutation Lifecycle Streams

Current mutation lifecycle RPCs:

- `SandboxService`: `CreateSandbox`, `TerminateSandbox`, `OpenSandboxShell`, `CloseSandboxShell`
- `ImageService`: `PullImage`, `PruneImages`
- `StackService`: `ApplyStack`, `TeardownStack`, `StopStackService`, `StartStackService`, `RestartStackService`

Contract:

- Stream emits zero or more progress payloads.
- Success path emits exactly one terminal completion payload.
- Failure path terminates with gRPC status error; completion payload is not emitted.
- Mutation streams that produce receipts return receipt linkage in completion payload and/or response metadata.

## Event/Tail Streams

Current event/tail RPCs:

- `ExecutionService.StreamExecOutput`
- `BuildService.StreamBuildEvents`
- `EventService.StreamEvents`

Contract:

- Events are delivered in source order.
- Stream may be long-lived.
- Terminality is payload-specific (for example execution exit/error payload) or stream closure.

## Backpressure and Disconnect Behavior

- Stream producers use bounded channels and preserve in-order delivery.
- Mutation lifecycle streams are materialized before response emission and are never intentionally dropped.
- Long-lived streams stop producing when receiver side is closed (disconnect/cancel).

## Reattach Behavior

- Execution output attach/reattach is keyed by execution/session identity.
- Repeat attach and post-restart reconciliation behavior is covered by daemon tests to ensure deterministic terminal outcomes.

## CI Guardrails

- `vz-runtime-proto` includes an explicit RPC mode map test (`runtime_v2_rpc_modes_are_explicit_and_stable`).
- Any RPC signature change (unary vs server-streaming) or newly added RPC now requires intentional classification in the contract map.
