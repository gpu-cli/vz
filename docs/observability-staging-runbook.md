# Observability Staging Validation Runbook

This runbook validates runtime observability before release gates are signed off.

## Preconditions

- `vz-runtimed` and `vz-api` binaries built from current commit.
- Staging environment exposes:
  - API metrics endpoint: `GET /metrics`
  - Daemon metrics snapshot file: `<runtime_data_dir>/runtimed-grpc-metrics.prom`
- Access to a Linux daemon path for storage/btrfs probes.

## Step 1: Generate Control-Plane Activity

The former `scripts/run-vz-linux-vm-e2e-hostboot.sh` activity generator is
[retired](retired-cli-workflows.md). It cannot exercise the API/daemon or produce
current observability evidence. Do not substitute an old CLI via `VZ_BIN`.

This step requires a current typed API/test-driver workload against the exact
staging daemon, with retained command, identity, timing, and terminal receipts.
Until that generator exists and succeeds, this runbook cannot receive readiness
sign-off. The supported local-Mac sandbox backend gate,
`scripts/run-sandbox-vm-e2e.sh --profile release --suite all`, is documented
[separately](sandbox-vm-e2e.md); it does not by itself establish the HTTP metrics,
storage activity, or observability assertions below.

## Step 2: Verify API Metrics Families

```bash
curl -fsS "$VZ_RUNTIME_API_BASE_URL/metrics" | tee /tmp/vz-api-metrics.prom
```

Must contain:

- `vz_api_http_requests_total`
- `vz_api_http_request_duration_seconds_bucket`
- `vz_api_http_request_duration_seconds_count`

## Step 3: Verify Daemon Metrics Snapshot

```bash
RUNTIME_DIR="${VZ_RUNTIME_DATA_DIR:-.vz-runtime}"
test -f "$RUNTIME_DIR/runtimed-grpc-metrics.prom"
grep -E "vz_runtimed_grpc_requests_total|vz_runtimed_grpc_request_duration_seconds_bucket|vz_runtimed_btrfs_health_status" \
  "$RUNTIME_DIR/runtimed-grpc-metrics.prom"
```

Must contain:

- `vz_runtimed_grpc_requests_total`
- `vz_runtimed_grpc_request_duration_seconds_bucket`
- `vz_runtimed_btrfs_health_status`
- `vz_runtimed_btrfs_health_last_probe_unix_seconds`

## Step 4: Dashboard Query Validation

In Prometheus/Grafana Explore, execute:

1. `sum by (route, method) (rate(vz_api_http_requests_total[5m]))`
2. `histogram_quantile(0.95, sum by (le, route, method) (rate(vz_api_http_request_duration_seconds_bucket[5m])))`
3. `sum by (rpc_method, grpc_status) (rate(vz_runtimed_grpc_requests_total[5m]))`
4. `histogram_quantile(0.95, sum by (le, rpc_method) (rate(vz_runtimed_grpc_request_duration_seconds_bucket[5m])))`
5. `vz_runtimed_btrfs_health_status`

Expected:

- Non-zero request rates after workload execution.
- p95 values produced for active routes/methods.
- btrfs health status present for `scrub` and `balance`.

## Step 5: Alert Rule Dry-Run

Evaluate candidate alert expressions from `docs/runtime-observability.md`:

1. `VzApiHigh5xxRatio`
2. `VzRuntimedHighGrpcFailureRatio`
3. `VzRuntimedHighCheckpointLatencyP95`
4. `VzRuntimedBtrfsHealthDegraded`
5. `VzRuntimedBtrfsProbeStale`

Expected:

- All rules evaluate without syntax/label errors.
- No paging alert is firing in baseline staging run.

## Step 6: Evidence Capture

Archive:

- API metrics snapshot (`/tmp/vz-api-metrics.prom`)
- daemon metrics snapshot (`runtimed-grpc-metrics.prom`)
- Grafana screenshots for both dashboards
- exact current workload commands and their raw operation/cleanup receipts

Store under:

```text
.artifacts/observability-staging/<timestamp>/
```

## Exit Criteria

- Current scoped activity generator is implemented and its retained run succeeds;
  a retired-helper rejection or historical hostboot summary is not evidence.
- Metrics families present and populated.
- Dashboard queries return expected series.
- Alert expressions evaluate cleanly and baseline is green.
- Evidence bundle archived for release review.
