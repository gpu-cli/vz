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

## Dashboard Specification (Staging/Prod)

### Dashboard: `vz-control-plane-overview`

Panels:

1. `API request rate`
- Query: `sum by (route, method) (rate(vz_api_http_requests_total[5m]))`
- Purpose: detect control-plane traffic shifts and dead routes.

2. `API 5xx ratio`
- Query:
  `sum(rate(vz_api_http_requests_total{status_class="5xx"}[5m])) / clamp_min(sum(rate(vz_api_http_requests_total[5m])), 1e-9)`
- Purpose: error-budget burn visibility for HTTP transport.

3. `API p95 latency by route`
- Query:
  `histogram_quantile(0.95, sum by (le, route, method) (rate(vz_api_http_request_duration_seconds_bucket[5m])))`
- Purpose: route-level latency regressions against SLO.

4. `Daemon gRPC request rate`
- Query: `sum by (rpc_method, grpc_status) (rate(vz_runtimed_grpc_requests_total[5m]))`
- Purpose: runtime service throughput and status mix.

5. `Daemon gRPC non-OK ratio`
- Query:
  `sum(rate(vz_runtimed_grpc_requests_total{grpc_status!="OK"}[5m])) / clamp_min(sum(rate(vz_runtimed_grpc_requests_total[5m])), 1e-9)`
- Purpose: transport-independent failure rate signal.

6. `Daemon gRPC p95 latency by method`
- Query:
  `histogram_quantile(0.95, sum by (le, rpc_method) (rate(vz_runtimed_grpc_request_duration_seconds_bucket[5m])))`
- Purpose: identify slow runtime RPCs affecting CLI/API UX.

7. `Btrfs maintenance health`
- Query: `vz_runtimed_btrfs_health_status`
- Purpose: single-pane signal for scrub/balance health state.

8. `Btrfs maintenance failures`
- Query: `increase(vz_runtimed_btrfs_health_failures_total[1h])`
- Purpose: detect recurring maintenance instability before user-facing failures.

### Dashboard: `vz-runtime-release-gates`

Panels:

1. `Checkpoint/restore path latency p95`
- Query:
  `histogram_quantile(0.95, sum by (le, rpc_method) (rate(vz_runtimed_grpc_request_duration_seconds_bucket{rpc_method=~".*/(CreateCheckpoint|RestoreCheckpoint|ExportCheckpoint|ImportCheckpoint)$"}[5m])))`

2. `Sandbox lifecycle latency p95`
- Query:
  `histogram_quantile(0.95, sum by (le, rpc_method) (rate(vz_runtimed_grpc_request_duration_seconds_bucket{rpc_method=~".*/(CreateSandbox|TerminateSandbox|OpenSandboxShell|CloseSandboxShell)$"}[5m])))`

3. `Release-critical non-OK ratio`
- Query:
  `sum(rate(vz_runtimed_grpc_requests_total{rpc_method=~".*/(CreateSandbox|CreateCheckpoint|RestoreCheckpoint)$",grpc_status!="OK"}[5m])) / clamp_min(sum(rate(vz_runtimed_grpc_requests_total{rpc_method=~".*/(CreateSandbox|CreateCheckpoint|RestoreCheckpoint)$"}[5m])), 1e-9)`

## Alert Policy and SLO Inputs

SLO inputs used by these alerts:

- Control-plane availability target: `99.0%` monthly.
- Control-plane error budget: `1.0%` monthly failed requests.
- Latency objective for interactive operations: p95 `< 500ms` (sandbox/checkpoint paths).
- Btrfs maintenance health objective: no sustained warning/error state.

Alert rules:

1. `VzApiHigh5xxRatio` (page)
- Condition:
  `sum(rate(vz_api_http_requests_total{status_class="5xx"}[10m])) / clamp_min(sum(rate(vz_api_http_requests_total[10m])), 1e-9) > 0.01`
- For: `10m`
- Maps to: availability/error-budget burn.

2. `VzRuntimedHighGrpcFailureRatio` (page)
- Condition:
  `sum(rate(vz_runtimed_grpc_requests_total{grpc_status!="OK"}[10m])) / clamp_min(sum(rate(vz_runtimed_grpc_requests_total[10m])), 1e-9) > 0.01`
- For: `10m`
- Maps to: daemon service reliability budget.

3. `VzRuntimedHighCheckpointLatencyP95` (ticket/page by severity)
- Condition:
  `histogram_quantile(0.95, sum by (le) (rate(vz_runtimed_grpc_request_duration_seconds_bucket{rpc_method=~".*/(CreateCheckpoint|RestoreCheckpoint)$"}[10m]))) > 0.5`
- For: `15m`
- Maps to: interactive save/restore latency objective.

4. `VzRuntimedBtrfsHealthDegraded` (page)
- Condition:
  `min(vz_runtimed_btrfs_health_status{component=~"scrub|balance"}) < 1`
- For: `15m`
- Maps to: storage integrity/maintenance objective.

5. `VzRuntimedBtrfsProbeStale` (ticket)
- Condition:
  `(time() - max(vz_runtimed_btrfs_health_last_probe_unix_seconds{component=~"scrub|balance"})) > 1800`
- For: `15m`
- Maps to: observability freshness gate.

## Staging Validation Checklist

See `docs/observability-staging-runbook.md` for the executable runbook.
