# vz 0.4.0: where it stands

Status: living summary, rewritten when a gate run changes it
Last measured: 2026-09-10, release candidate `0.4.0-rc2`, Apple-silicon macOS 26.3.1

This is the short answer to "what's left". The gate itself is
[GOAL-0.4.0.md](GOAL-0.4.0.md); this file says which of its claims are proved,
which are disproved, and what is in the way.

## The one-line version

The **harness is finished** -- all eighteen topology-lane scenarios have real
checks, and nothing in that lane reports "needs provisioned Machines" any more.
What remains is **product work**, and the gate now names it precisely instead of
failing in a heap.

## What the last hardware run proved

Topology lane, clean-provision phase, against a locally signed release candidate
with a registered native macOS target. **Eleven of fifteen sub-checks pass.**

| Passing on hardware | |
|---|---|
| criterion 1 | three concurrent Environments, no collision |
| criterion 2 | mixed Linux + native macOS topology and status |
| criterion 15 | CLI/API agreement, including live gRPC and the status field set |
| criterion 21 | legacy CLI removal, bare help, bootstrap rules |

Criterion 2 is the notable one: a native macOS Machine boots, is supervised, and
reports correctly beside Linux Machines in one Environment.

## What is in the way, in the order it blocks things

Each of these is a filed issue, and each is the reason a specific criterion
cannot pass. None of them is a harness gap.

### P0

1. **There is no reconciliation.** `vz up` refuses *every* ProjectDefinition
   change before admission -- `project definition drift`, `StackError::InvalidSpec`.
   No plan is derived, no durable claim is taken, and a mutable field change gets
   the same code as an immutable one. Neither of criterion 22's two normative
   sub-documents has an implementation subject at all: `admit_reconcile_round`,
   `ReconcileInputSnapshot` and `effective_digest` exist nowhere in `crates/`.
   *Blocks criterion 22 entirely.*

2. **No machine-scoped lifecycle operation.** Every runtime teardown primitive is
   fenced on a persisted Environment-wide `EnvironmentLifecycleOperation` whose
   structure check requires one machine step per Machine, so `vz delete --machine`
   resolves a fork and then refuses. The refusal is deliberate: a partial teardown
   leaks a host Docker context and a runtime store *while the ownership rows claim
   reclamation*. *Blocks criterion 23's delete clause.*

### P1

3. **Native macOS Machines take no fabric address.** The declaration is accepted
   at every layer now and the Environment comes up; the guest just never gets an
   address on the subnet. A Linux guest reads its address off the kernel cmdline;
   a macOS guest is given it over the agent channel during readiness, and that
   half does not work. *Blocks criterion 5's crossing and criterion 12's
   cooperating Linux-to-macOS pair.*

4. **Guests resolve through public DNS.** `resolv.conf` comes up as
   `[1.1.1.1, 8.8.8.8]` instead of the Environment's declared gateway. Resolution
   is not broken -- it works, through the wrong resolver, which is the shape that
   passes a smoke test and fails the criterion. *Blocks criterion 6's split-DNS
   clause and half of criterion 8's resolve clause.*

5. **A refused `vz up` mutates the worktree**, minting `.git/vz/workspace-id`.
   Fail-before-mutation is stated for definition changes and for bootstrap, and
   this is the same contract. It leaves a binding artifact a later Up will adopt.

6. **Capability negotiation is a rubber stamp.** `negotiated_capabilities =
   requested_capabilities.clone()`, so a Machine asking for `snapshot` is granted
   it while the capability matrix says PLANNED. Every consumer that trusts
   capability discovery -- help, docs, site copy, status -- inherits the false
   claim. *Blocks criterion 18's snapshot clause.*

7. **No `SecretBinding` exists** -- not in the project schema, not in
   `vz-runtime-contract`. The gate check for it is written and waiting: it plants
   a high-entropy sentinel through the CLI environment, reads it back only as a
   digest, and sweeps seven artifact groups for the literal bytes.
   *Blocks criterion 18's secrets clause.*

8. **A guest RPC was added without bumping `AGENT_PROTOCOL_REVISION`.** The
   handshake that exists to refuse a stale guest passed, and the guest answered
   `Unimplemented` deep inside `up` instead. The revision is a hand-maintained
   constant with nothing tying it to the surface it describes.

## What the gate will not tell you yet

- **The fork check has never run against real VMs.** Two bounds a first real run
  must settle: `FORK_SPEEDUP_MIN = 2.0` and `FORK_FREE_SPACE_FRACTION = 0.25`.
  The free-space window spans the whole `up`, so it carries the fork's own boot
  writes. The check records the cold Up's wall time and free-space delta beside
  the fork's so those bounds can be judged from evidence; revising them is a
  product decision, not a repair to the measurement.
- **No aggregate run has completed.** The four-phase gate needs a real Mac sleep
  between pre-sleep and post-wake, so it is an attended run.
- **The pinned macOS template lives in `/private/tmp`.** Rebuilding it costs an
  IPSW download and an administrator authorisation. It is one purge away from
  gone and should be moved somewhere durable.

## Measured facts worth keeping

| | |
|---|---|
| `clonefile(2)` of an 80 GiB template holding 32.9 GiB | **0.029 s, 28 KB of volume free space** |
| the same file, held open by a writer mid-`fsync` | clone exit 0 in **0.077 s** |
| a Machine's Docker `data.img` | 64 GiB logical, **29 MB of data in 78 extents** |
| APFS `st_blocks` of a clone versus its parent | **exactly equal** -- which is why the criterion measures free space |

The last row is the one that keeps being rediscovered. A per-file allocated-size
comparison reads a *correct* copy-on-write clone as a deep copy. Criterion 23 was
rewritten around it once and the fork check asserts the per-file equality
alongside the free-space bound so it cannot be undone by accident.
