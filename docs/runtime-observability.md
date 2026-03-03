# Runtime Observability Surfaces

`vz` now exposes Prometheus-compatible metrics for both control-plane transports.

## API (`vz-api`)

- Endpoint: `GET /metrics`
- Content-Type: `text/plain; version=0.0.4; charset=utf-8`
- Request IDs:
  - Incoming `x-request-id` is propagated end-to-end.
  - If missing, `vz-api` generates one and returns it via response header `x-request-id`.
- Metrics:
  - `vz_api_http_requests_total{method,route,status_class}`
  - `vz_api_http_request_duration_seconds{...}` histogram buckets, sum, count
- Cardinality controls:
  - Route labels are normalized to stable templates (for example
    `/v1/sandboxes/{sandbox_id}/shell/open`).

## Daemon (`vz-runtimed`)

- Snapshot file: `<runtime_data_dir>/runtimed-grpc-metrics.prom`
  - Default runtime data dir: `.vz-runtime`
  - Default snapshot path: `.vz-runtime/runtimed-grpc-metrics.prom`
- Metrics:
  - `vz_runtimed_grpc_requests_total{rpc_method,grpc_status}`
  - `vz_runtimed_grpc_request_duration_seconds{...}` histogram buckets, sum, count
- Cardinality controls:
  - `rpc_method` uses the gRPC method path (for example
    `/vz.runtime.v2.SandboxService/CreateSandbox`).
  - Outcome labels use gRPC status code strings.

## Validation Hooks

- `cargo test -p vz-api` verifies:
  - request-id header behavior
  - `/metrics` endpoint and metric families
- `cargo test -p vz-runtimed` verifies:
  - gRPC metric registry rendering
- daemon metrics snapshot file creation on server start

## Btrfs Health Metrics (Daemon)

`vz-runtimed` now publishes btrfs maintenance probe metrics from its maintenance loop:

- `vz_runtimed_btrfs_health_status{component="scrub|balance"}`
  - values: `1` healthy, `0` warning, `-1` error, `-2` unsupported
- `vz_runtimed_btrfs_health_failures_total{component="scrub|balance"}`
- `vz_runtimed_btrfs_health_last_probe_unix_seconds{component="scrub|balance"}`

Probe state transitions are also persisted as:

- structured runtime events (`drift_detected`, category `btrfs_health`)
- receipts (`operation=btrfs_health_probe`, `entity_type=maintenance`)

Recommended alerts:

- fire when `vz_runtimed_btrfs_health_status{component="scrub"} < 1`
- fire when `vz_runtimed_btrfs_health_status{component="balance"} < 1`
