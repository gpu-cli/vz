# Retired CLI helper workflows

Status: removed from the 0.4 development CLI; historical evidence is preserved.

These scripts are migration-error entry points, **not executable workload or
certification workflows**:

- `scripts/run-vz-linux-vm-e2e.sh`
- `scripts/run-vz-linux-vm-e2e-local.sh`
- `scripts/run-vz-linux-vm-e2e-hostboot.sh`
- `scripts/run-vz-linux-hostboot-command.sh`
- `scripts/run-linux-daemon-release-gate.sh`

Their former implementations depended on removed `create`, `vm`, and other
infrastructure CLI paths. They are retired before build, daemon, state, or VM
effects. They do not select another executable from PATH or honor `VZ_BIN` as
a way to restore the old lifecycle. A help request does not make a retired
workflow supported. Use the structured migration response to identify the
retirement; do not treat an old `failed=none` summary as current evidence.

## What can be run now

The supported local Apple-silicon **sandbox backend** gate is:

```bash
./scripts/run-sandbox-vm-e2e.sh --profile release --suite all
```

It builds/signs its test drivers and exercises the applicable vz-managed Linux
backend. Follow [its prerequisites and artifact contract](sandbox-vm-e2e.md).
It does not depend on the retired public VM parser.

This pointer is not a replacement certification for the old high-level CLI/API
lane, Linux-native portability, observability soak, full Docker/Compose/buildx,
native macOS, or the complete Developer Environment lifecycle. Those scenarios
need their own typed, exactly owned API/test-driver implementation and retained
evidence. If the required workload driver does not yet exist, that runbook or
release gate remains incomplete; do not substitute another Machine, daemon,
guest client, old binary, or adjacent green test.

The current DEV CLI implements `up`, `exec`, `status`, and `stop`; `delete`
is absent and complete Up reconciliation/readiness remain unfinished. Up's
Linux-on-macOS adapter requires an explicitly configured verified catalog and
never promotes Engine-only evidence to Developer readiness. Bare `vz` prints
static help without state discovery. The
[product contract](developer-environments.md) defines the completed five-verb
workflow, and the [release goal](../planning/developer-environments/GOAL-0.4.0.md)
defines its acceptance evidence.

## Historical material

Dated game-day logs and archived VM contracts preserve what was run or designed
at that time. A retirement banner changes their interpretation, not their
historical observations. Their old commands, hidden aliases, hostboot summaries,
and test names must not be copied into a current release invocation.

The removed CLI-to-API success tests have an explicit
[coverage handoff](../crates/vz-cli/tests/fixtures/retired-api-cli-coverage.md).
Their new transport-rejection checks prove absence of effects, not backend API
or workload success. The immutable released v0.3.20 migration fixture and its
recursive help traversal also remain separate pending release inputs.
