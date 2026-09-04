# Backend verification for agent work

Use these requirements when changing runtime/backend behavior or certifying a
release. Documentation-only changes are validated through their references and
examples; they do not require unrelated VM runs.

Completion requires real end-to-end verification on the relevant
host×Machine-target backend. For topology work, also pass the aggregate
Environment gate in
[GOAL-0.4.0.md](../planning/developer-environments/GOAL-0.4.0.md).
Keep a runtime/backend task open if its applicable gate has not passed; record
what was verified and what remains blocked.

## Evidence boundaries

- On macOS, Linux-target verification runs in a local **vz-managed Linux
  Machine**, not an ad-hoc external SSH host. Until the five-verb surface lands,
  focused backend harnesses may use `vz vm ...`. Release 0.4 evidence must use the
  installed public CLI/API and prove `vz vm` is no longer executable.
- Native macOS changes need the native macOS-on-macOS gate. Docker evidence from
  a neighboring Linux Machine does not certify a native macOS target.
- Linux-on-Linux, Linux-on-Windows, and Windows-on-Windows claims each require
  their own release-built host×Machine-target conformance evidence.

## Relevant suites

Run from the repository root and attach artifact paths and logs to the issue:

| Change | Required evidence |
| --- | --- |
| Container/stack runtime | `scripts/run-sandbox-vm-e2e.sh --suite all` and artifact logs |
| Linux-target Docker | Ship and run `scripts/run-linux-docker-e2e.sh --suite all` from the host when the Docker wave lands; prove youki is the only OCI runtime binary present in the target |
| btrfs portability | `scripts/run-linux-btrfs-e2e.sh`, with `.artifacts/linux-btrfs-e2e/<timestamp>/summary.txt` and logs |

The Docker suite is a required deliverable, not an already-present harness.
This requirement includes `vz-5in`, `vz-yr9`, `vz-k3v`, and `vz-7ez`.
Use the product/release gate for native macOS and aggregate topology evidence;
passing a focused suite does not replace those gates.
