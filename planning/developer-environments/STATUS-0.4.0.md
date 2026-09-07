# vz 0.4.0 status

Status: current picture as of 2026-09-07. Normative source:
[`GOAL-0.4.0.md`](GOAL-0.4.0.md). This document says what exists, what is
proven, and what remains. It is written to be read on its own.

## The one-line answer

The release gate now exists and runs, and it says FAIL. That is the honest
result: of the 85 scenarios the contract requires, none yet passes through the
aggregate. Two of the four lanes are unimplemented, the Docker lane's own
coverage has known gaps, and — found 2026-09-07 — **the Docker lane could not be
invoked by the gate at all**: the contract gives it `["--suite", "all"]`, and
the harness rejects that argv until eight more required options are supplied
(`vz-ao8`). Only `--dry-lanes` runs had taken that path, and a dry lane
substitutes its result without starting the process, so the rejection had never
been observed. That is the gap between a composed candidate passing every suite
directly and zero scenarios passing the aggregate.

Seven and a half of those eight now land. Five are facts about the candidate
and are derived from it by a `linux_docker` argv contract; `--tmux` is resolved
by the gate like the Docker clients; the registry archive and layout are
acquired from `config/docker-registry-artifact-v3.1.1.json`, reproducing the
byte-identical artifact the candidates use. `--ssh-packages` is the remainder:
its pin now carries a verified `repository_path` for every locatable row and an
acquirer fetches them, but the base-image extracts, one derived stanza and about
38 KB of generated provenance still stand between that and a supplied option.

The Docker lane owns 66 of the 85 scenarios — 63 `docker.*` plus three `gate.*`
— so this one defect stands in front of 78% of the release gate. The topology
lane owns 18 and native-macOS owns 1.

What changed recently is that the gate, its validator, its frozen inputs and
three of its four lanes are real, so every remaining piece now lands against a
mechanical verdict instead of accumulating as separate focused passes.

## Tracked work

| State | Count |
|---|---|
| Closed | 63 |
| In progress | 48 |
| Open | 100 |

## What is proven today

Everything below is DEV evidence from an installed, signed, local Apple-silicon
build driving public interfaces. None of it is release certification: every
record carries `aggregate_release_certified: false`.

**Rust gates.** `cargo fmt --check`, `cargo clippy --workspace --all-targets
--all-features -- -D warnings` and `cargo nextest run --workspace
--all-features` all pass (2,914 tests). The clippy gate is the exact command the
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
  is still a stub; the Docker lane has ten uncovered scenario IDs, needing four
  gap-suites: `mounts` for the five storage IDs, `netpolicy` for published ports
  and network cleanup, `concurrency` for concurrent clients, and `isolation` for
  the two cross-Environment IDs. Only the first two are blocked on product
  features; concurrency and isolation need a three-Environment topology and
  nothing else.
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
   network, endpoint or workspace projection, and now also refuses declared host
   relays and non-offline egress rather than admitting a boundary it cannot
   apply. Step 1 of [`NETWORK-INCREMENT-PLAN.md`](NETWORK-INCREMENT-PLAN.md) has
   landed, with its typed records, state-store schema v10 and both migration
   barriers, and so has the first half of step 2's substrate: the file-handle
   network attachment and a VM that can hold a list of NICs instead of one. The
   switch itself, the guest addressing, and the three admission gates remain.
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
Twenty-one installed candidates took it from refusing outright to a run that
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

Candidates 17 to 21 continued the same pattern, each exposing one real coupling:

- The recovery module's monitor subclass builds its own state and predated the
  probe cache the fast path added, and a non-idempotent `stop()` then masked that
  real failure behind an exclusive write. Candidate 20 executed all ten suites
  before failing there, so the fast path holds across the whole composition.
- Candidate 21 failed in **limits** on a 21 millisecond race. The validator
  requires every workload envelope to open after the health probe's first sample
  has finished, and the suite satisfied that only by assuming its first command
  was slow. On a warm Engine it was not, and the first bracket opened 21 ms
  early. The probe now announces its first completed sample and the suite waits
  for it, so the precondition is checked rather than assumed. Candidate 20 had
  won the same coin flip.

Candidate 23 passed all ten composed suites with no non-zero exit outside the
run's own negative assertions, so the fast path and every coupling above hold
together across one provisioning.

**lifecycle** was the last suite excluded from `--suite all`, because its
evidence is a youki runtime-audit journal bounded at 2,048 records per Machine
and a whole-run window overruns that on sentinel sampling alone — about 1,488
records per Machine in 25 minutes, measured on candidate 21. Two changes compose
it (`vz-mzs.7.1.16`), unit-tested but **not yet proven by a composed candidate**;
the count of audit records on the lifecycle Machine is what will prove it:

- The monitor no longer samples the Machine running the workload. Both liveness
  assertions already excluded that Machine — `close_interval` subtracts it and
  `check_interval` skips it — so the per-second exec into it produced evidence
  no check ever read while spending the exact journal lifecycle needs. Sampling
  resumes the moment another Machine is active, where it is a sibling and its
  samples are used, so no sibling liveness is lost anywhere in the run.
- A composed run opens the audit window immediately before the lifecycle suite
  and closes it immediately after, with the monitor paused for the capture
  alone. That is complete evidence for the suite, because it removes its own
  containers and image before it returns. `--suite lifecycle` keeps the
  whole-run window it always had.

`lifecycle` now runs second to last in `SUITE_ORDER`, before `recovery`, which
must stay last because it cycles Stop/Up and replaces the monitor. A composed
run therefore carries `--tmux`, and may pin `--container-fixture`.

The limits window coupling recorded here earlier is closed: the suite now runs
180 one-second samples, and the reasoning is recorded beside the constant.

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
