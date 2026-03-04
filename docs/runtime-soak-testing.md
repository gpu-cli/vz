# Runtime Soak Testing and Leak Gates

This document defines the long-duration soak gate for runtime workloads.

## Harness

Use:

```bash
scripts/run-runtime-soak.sh \
  --workload-cmd "<workload command>" \
  --daemon-pid <vz-runtimed-pid> \
  --iterations 120 \
  --max-rss-growth-kb 131072 \
  --max-fd-growth 256 \
  --report .artifacts/runtime-soak/latest.json
```

## Baselines and Thresholds

- Baseline RSS: `VmRSS` sampled from `/proc/<pid>/status` at iteration start.
- Baseline FD count: sampled from `/proc/<pid>/fd`.
- Regression gates:
  - fail when RSS growth exceeds `--max-rss-growth-kb`.
  - fail when FD growth exceeds `--max-fd-growth`.
  - fail when daemon PID exits mid-soak.

Defaults:

- iterations: `120`
- max RSS growth: `131072` KiB (128 MiB)
- max FD growth: `256`

## Suggested Workloads

1. Linux daemon lifecycle loop:
```bash
VZ_BIN=/tmp/vz-target-e2e/debug/vz scripts/run-vz-linux-vm-e2e-hostboot.sh --profile debug
```

2. Portability stress loop:
```bash
VZ_BIN=/tmp/vz-target-e2e/debug/vz scripts/run-vz-linux-vm-e2e-hostboot.sh --profile debug --run-btrfs-portability
```

## Readiness Gate Contract

- Soak job writes JSON report to `.artifacts/runtime-soak/`.
- CI/release gate must parse `passed=true`.
- Any failed report blocks readiness sign-off until triaged.
