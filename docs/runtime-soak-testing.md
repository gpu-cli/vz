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

## Workload status after CLI retirement

The formerly suggested lifecycle and portability loops used
`scripts/run-vz-linux-vm-e2e-hostboot.sh`, which is now a retired migration-error
entry point, not a workload generator. Do not point `VZ_BIN` at an older CLI or
count its rejection as a successful soak iteration. See
[retired workflows](retired-cli-workflows.md).

Supply an actual, exactly scoped typed API/test-driver workload for the selected
Linux daemon and retain its raw operation/cleanup receipts. Until that workload
is implemented and exercised, the corresponding soak gate remains incomplete.
The current local-Mac sandbox backend gate is
`scripts/run-sandbox-vm-e2e.sh --profile release --suite all`; follow its
[prerequisites](sandbox-vm-e2e.md). A passing sandbox run does not certify this
long-duration Linux-daemon or portability soak.

## Readiness Gate Contract

- Soak job writes JSON report to `.artifacts/runtime-soak/`.
- CI/release gate must parse `passed=true`.
- Any failed report blocks readiness sign-off until triaged.
