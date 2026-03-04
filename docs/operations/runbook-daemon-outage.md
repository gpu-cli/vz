# Runbook: Daemon/API Outage

## Trigger Conditions

- CLI returns daemon unavailable or startup timeout.
- API requests fail with `daemon_unavailable`.
- `/metrics` is unreachable or stale.

## Immediate Triage

1. Confirm process/socket health:
```bash
ps -ef | rg "vz-runtimed|vz-api"
ls -l .vz-runtime/runtimed.sock
```
2. Check daemon/API logs for startup or bind failures.
3. Verify state DB path and runtime data directory are writable.

## Recovery Steps

1. Restart daemon with explicit paths:
```bash
vz-runtimed --state-store .vz-runtime/stack-state.db --runtime-data-dir .vz-runtime --socket .vz-runtime/runtimed.sock
```
2. Restart API with explicit daemon socket:
```bash
vz-api --state-db .vz-runtime/stack-state.db --daemon-socket .vz-runtime/runtimed.sock
```
3. Re-run readiness probes:
```bash
curl -fsS "${VZ_RUNTIME_API_BASE_URL:-http://127.0.0.1:8080}/v1/capabilities"
curl -fsS "${VZ_RUNTIME_API_BASE_URL:-http://127.0.0.1:8080}/metrics" | rg "vz_api_http_requests_total"
```

## Validation Exit Criteria

- Capabilities endpoint returns 200.
- Metrics endpoint returns expected families.
- `vz vm linux list --state-db ...` returns without daemon transport errors.

## Escalation

- If service is not restored within 15 minutes, escalate to runtime owner and incident commander.
- If state DB corruption is suspected, stop writes and switch to recovery playbook.
