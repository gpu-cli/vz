# vz 0.4.0 status

Status: current picture as of 2026-09-07. Normative source:
[`GOAL-0.4.0.md`](GOAL-0.4.0.md). This document says what exists, what is
proven, and what remains. It is written to be read on its own.

## The one-line answer

The release gate now exists and runs, and it says FAIL. That is the honest
result: of the 85 scenarios the contract requires, none yet passes through the
aggregate, because two of the four lanes are unimplemented and the Docker lane's
own coverage has known gaps. What changed recently is that the gate, its
validator, its frozen inputs and three of its four lanes are real, so every
remaining piece now lands against a mechanical verdict instead of accumulating
as separate focused passes.

## Tracked work

| State | Count |
|---|---|
| Closed | 38 |
| In progress | 31 |
| Open | 68 |

## What is proven today

Everything below is DEV evidence from an installed, signed, local Apple-silicon
build driving public interfaces. None of it is release certification: every
record carries `aggregate_release_certified: false`.

**Rust gates.** `cargo fmt --check`, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` and `cargo nextest run --workspace
--all-features` all pass (2,895 tests). The clippy gate is the exact command the
release contract names, and it went from 2,733 findings to zero.

**Docker slices.** Installed candidates pass compose, build, artifacts, parallel,
ssh, lifecycle, images, registry, handshake, limits and recovery. The registry
slice proves wrong-CA rejection, invalid-password rejection, unauthenticated
push denial, an authenticated login with a server-side route witness, push,
pull-by-digest, export re-verification, independent receipt replay, secret
canary scans and exact cleanup, across three Machines at the same private
authority with the fourth as a neighbor sentinel.

**Capability honesty.** A checked-in host×target×profile matrix records what each
pair actually supports, a Rust test fails if the capability enum drifts from it,
and a linter binds all 133 capability claims across README, docs, site, skills
and CLI help to that matrix. No capability is labeled ACTIVE, because no 0.4
release exists.

**The gate itself.** The entry point, the read-only validator, the release
candidate builder, fifteen JSON schemas and the frozen-input drafts exist. A run
captures host, toolchain and client facts, listener, process, socket and
Docker-context inventories before and after, and a leak diff; sleep and wake are
proven by the discontinuity between the monotonic and uptime-raw clocks bound to
a nonce and boot session. The validator reproduces the gate's verdict
independently from the retained evidence.

## What the gate reports, and why

The dry run's 109 findings are all correct:

- **85 scenarios missing.** The topology lane proves six sub-checks of the CLI
  criteria and honestly reports the rest not implemented; the native-macOS lane
  is still a stub; the Docker lane has fourteen uncovered scenario IDs.
- **Inputs are drafts.** Four frozen-input files are `draft_unverified` and
  eleven contract values are still null, mostly native-macOS pins that are not
  yet knowable.
- **The candidate is development evidence.** It is locally test-signed, not
  Developer-ID signed and notarized, and it was built from a dirty checkout.
- **Sleep and wake were not observed**, because the dry run substitutes lanes.

## What remains, in rough order of size

1. **Network fabric and host boundaries.** Declared private paths, an
   Environment-owned switch, host imports and exports, published ports, egress
   policy, split DNS, TLS ingress, faults with numeric tolerances, peering with
   expiry, and the exhaustive denial matrix. Today `vz up` rejects any declared
   network, endpoint or workspace projection. The increment is designed in
   [`NETWORK-INCREMENT-PLAN.md`](NETWORK-INCREMENT-PLAN.md).
2. **Native macOS Machines.** Ownership adaptation, workspace, exec and
   services, mixed Linux and macOS topologies, and the release gate for them.
   The local setup path is in progress and its evidence still records a failure.
3. **Topology reconciliation and streaming.** Generation fencing, replica
   identity, crash-atomic batches and private runtime ownership.
4. **The remaining Docker coverage.** Bind mounts and published ports are
   blocked on the two product features above; concurrency and cross-Environment
   isolation are not blocked but need a three-Environment topology.
5. **Migration and GA.** Upgrade from the pinned v0.3.20 fixture, injected
   failure and rollback, uninstall preservation, and a Developer-ID signed and
   notarized distribution whose pre-signing digests match the local candidate.
6. **Freezing the inputs and certifying.** Only once the fixtures and harness
   are stable, and last of all the staged clean-provision, persisted-recovery
   with a real hardware sleep, and final-cleanup run against one candidate.

## Composing the Docker lane

`--suite all` now provisions the topology once and walks the suites in order.
Sixteen installed candidates took it from refusing outright to a run that
executed all ten composed suites, handshake through recovery, with no workload
error. Every candidate exposed exactly one real cross-suite coupling, each fixed
and committed with its reason:

- The executing suite was not threaded through the driver helpers, so a composed
  run built without a builder mapping and refused its own replays.
- The builder registry, the BuildKit object names and the health evidence
  directory were all keyed without the suite, so the second suite to want them
  collided with the first.
- Builder identity was hashed in two places that then disagreed.
- Compose recipe timeouts had no margin over the fixture's own health intervals,
  and the parallel barrier could not tolerate builder startup skew.
- Parallel source vertices were modelled as an ordered stream, but BuildKit
  replays a vertex's history and four concurrent slots genuinely overlap and
  supersede solves of one source. Three assertions the rows cannot support were
  narrowed, each with its reason, and the new rule was checked against every
  source vertex of a real candidate before running.
- The health container was recorded on the shared fixture ownership row.
- Owned builders were not reconciled before the recovery suite restarts the
  Machine they live in, and the final-cleanup certainty guard, which requires a
  stopped monitor, was being applied to that mid-run removal.

Two couplings remain, both tracked and both honest about what they cost:

- **lifecycle** cannot be composed, because its evidence is a youki
  runtime-audit journal bounded at 2,048 records per Machine that must be
  enrolled before any owned mutation and captured only after the monitor stops.
  A composed run's sentinel sampling alone exceeds that bound. It is excluded,
  its sixteen scenario IDs are reported missing rather than proven with a broken
  journal, and `--suite lifecycle` still proves them.
- **limits** runs a fixed sixty one-second health samples, giving a 57 second
  bracket that its workload outgrows on a Machine which has already run eight
  other suites, though it fits standalone. The gate requires at least sixty
  seconds of probes with zero failures, so a window that covers the workload is
  strictly more evidence; making it so is tracked rather than rushed.

## How to run what exists

```bash
scripts/build-vz-0.4-release-candidate.sh --output <new dir> --version 0.4.0-dev
scripts/run-vz-0.4-release-gate.sh --suite all --release-dir <dir> --run-id <id> \
  --docker <path> --compose-plugin <path> --buildx-plugin <path>
scripts/validate-vz-0.4-evidence.sh <evidence>/manifest.json
```

The Docker lane can also be driven directly for one suite or composed:

```bash
scripts/run-linux-docker-e2e.sh --suite registry ...   # one DEV slice
scripts/run-linux-docker-e2e.sh --suite all ...        # every suite, one topology
```
