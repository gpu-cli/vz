# vz 0.4.0: where it stands

Status: living summary, rewritten when a gate run changes it
Last measured: 2026-09-10, release candidate `0.4.0-rc3`, Apple-silicon macOS 26.3.1

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
with a registered native macOS target and guest bundles built from source.
**Thirteen of sixteen sub-checks pass.**

| Passing on hardware | |
|---|---|
| criterion 1 | three concurrent Environments, no collision |
| criterion 2 | mixed Linux + native macOS topology and status |
| criterion 15 | CLI/API agreement, including live gRPC and the status field set |
| criterion 19 | clean install, upgrade from the pinned v0.3.20 fixture, injected migration failure and rollback, uninstall |
| criterion 21 | legacy CLI removal, bare help, bootstrap rules |

Criterion 2 is the notable one: a native macOS Machine boots, is supervised, and
reports correctly beside Linux Machines in one Environment. Criterion 19 is the
most substantive: it opens the restored store with the real v0.3.20 daemon.

Zero daemons leak. Earlier runs left eight to ten alive, which on this host
exhausts the macOS virtual-machine cap and makes every later Environment fail
with `VZErrorDomain:6` -- three criteria failed that way before it was found,
with evidence blaming the Environment rather than the daemons nobody stopped.

## What is in the way, in the order it blocks things

Rewritten 2026-09-10 after a night of product work. Five defects below were
fixed; four things previously listed here turned out not to be product defects
at all, and that is recorded in the next section because the mistake is more
instructive than the fixes.

### Fixed

- **Fork-scoped delete** (`vz delete --machine <machine>@<label>`). Needed a
  machine-scoped lifecycle operation: ~900 production lines across five crates.
  The load-bearing choice is that a scoped operation NEVER ATTACHES, because a
  stable Environment may not retain an `active_operation_id` -- an attaching
  begin would have forced the Environment to `Deleting` while its other Machines
  keep serving. Criterion 23's last blocker.
- **Native macOS Machines received no endpoint name table.** Linux gets
  `vz.host.{N}` on the kernel cmdline and writes `/etc/hosts`; macOS got only its
  address. Every Linux sibling could reach a macOS endpoint by name and the macOS
  Machine could reach none of theirs -- an asymmetry directly under criterion 5's
  "in both directions".
- **`vz up`/`stop`/`delete` discarded the error envelope** for every failure
  decided inside the operation stream under `--json`: nonzero exit, empty stderr.
  A criterion-15 agreement violation far wider than the host-export port
  collision that exposed it.
- **Capability negotiation consulted nothing.** No code read
  `host-target-capabilities-v0.4.json` at all; the only rejection anywhere was a
  hand-kept `gui || windows_console` check in two places. There is now a matrix
  consumer that refuses an unadvertised capability BEFORE admission, naming it
  and its status.
- **A refused `vz up` minted and persisted `.git/vz/workspace-id`** before the
  CLI even connected to the daemon, so EVERY refusal after definition discovery
  left the artifact -- not only the one the gate exercised. Minting and
  publishing are now separate, with the publish after admission.
- **`vz up --fork-from` could not succeed against any Environment that had
  finished coming up.** Eight distinct refusals, found one at a time by running
  the installed binary against real Machines and fixing what it hit. They are
  one bug wearing eight faces: forking introduced the first Machine that is a
  *runtime* object rather than a *declared* one, and every layer of the Up path
  had encoded "Machine ⇒ declared in `vz.json` ⇒ present since the Environment
  was created". Each layer answered a per-Machine question with an
  Environment-wide fact, or looked a fork up in a definition it is never in.

  | # | What it said | What it was |
  |---|---|---|
  | 1 | `Ready requires every Machine to be Ready` | `validate` counted forks as declared |
  | 2 | `cannot reconstruct an unknown previously active Machine` | "never started" read off the Environment's generation |
  | 3 | `owned resource machine_runtime_store... not found` | reserve-or-require decided per Environment |
  | 4 | `unexpected state change during Machine reservation` | expected snapshot rebuilt only when fresh |
  | 5 | *(pre-empted by a sweep)* | store opened not created, and no digest to create it with |
  | 6 | `persisted Machine specification is missing` | the declaration looked up under a name no fork has |
  | 7 | `Machine artifact pin is missing` | recovery refuses to create; a fork must inherit its parent's |
  | 8 | `Machine absent from operation` | no step in a generation the fork did not exist in |

  **3288 workspace tests agreed forking worked throughout**, because every fork
  fixture forks an Environment from `instantiate_environment` — `Creating`, with
  all Machines `Creating`, the one lifecycle state a real fork is never taken in.
  The fixture chose the state that made the code pass. `ready_fixture` closes
  that gap. This is the argument for AGENTS.md's installed-evidence rule,
  demonstrated eight times in one night.

  The detail below is the first of the eight, kept because it is the one that
  decided the rule the other seven followed. (P0, found the first time the fork check reached real
  Machines). `Ready` demanded that *every* Machine be Ready; a fork is minted
  `Creating` because it has not booted; `fork_machine_in_environment` validates
  `before + plan` inside the transaction. So a warm parent -- the only kind worth
  forking -- was refused by construction. 3248 workspace tests agreed forking
  worked, because every fork fixture forks an Environment from
  `instantiate_environment`, which is `Creating` with all Machines `Creating`:
  the fixture chose the one lifecycle state a real fork is never taken in.

  The fix reads the field whose own documentation already decided the question.
  `MachineInstance.fork` is *"the field that separates a declared Machine from a
  runtime one, and every definition-versus-instance comparison reads it"*; the
  Ready invariant was the one comparison that did not. Ready now means every
  **declared** Machine is Ready. Moving the Environment to `Reconciling` for the
  duration was the obvious alternative and is worse: it announces a
  reconciliation over a Machine reconciliation does not consider, and it
  serialises forks through an Environment-wide state, so every sibling worktree
  would watch the shared Environment change because someone else took a copy --
  the exact interference forking exists to remove.

### Still open

1. **`vz up --fork-from` works, and criterion 23 is one assertion from passing**
   (`vz-5v8.8`). Twelve refusals were found by running the installed binary on
   real Machines, and eleven are fixed. What the last hardware run proves, all
   of it on real VMs: the fork boots and joins a fabric its parent is already
   forwarding on; it holds its own machine_id, incarnation, fabric address, MAC,
   Docker context and — after a defect found and fixed in this run — its own
   Docker **engine id**; its image store answers for **every digest its parent
   held** with **zero image pulls**; a volume created on the parent after the
   fork is absent from it; the parent's sentinel is byte-identical afterwards;
   reconcile leaves the fork alone; the fork survives a plain `vz up` keeping
   its lineage; a second fork with an explicit `--as` succeeds; `vz exec`
   without `--machine` refuses and names all three candidates.

   And the claim the criterion exists for: **the fork's disk shares its parent's
   physical blocks, 9 of 9 sampled offsets.** Copy-on-write, measured directly
   rather than inferred, for the first time.

   The one remaining failure is `FORK_SPEEDUP_MIN`, and it is an assumption
   rather than a defect: the fork reached ready in 55.6 s against 41.3 s cold,
   0.74x where 2x is required. A fork's Up clones, boots and replays a journal,
   while the cold control is the same bare definition that pulls no images — so
   the fork pays more and saves nothing *measurable against that baseline*. The
   bound was NOT lowered to make it pass; the write-up asks for a cold control
   that reaches the warm state the fork inherits.

2. **A fork's Docker disk is cloned from a live filesystem** (P0, `vz-5v8.7`).
   The eleventh and last refusal, and the one that matters: the fork now
   **boots**, joins the running fabric, holds its own identity and address, and
   is then refused inside the guest — `Docker filesystem is not positively
   clean; automatic repair is forbidden`. `seed_forked_docker_disk` clones the
   parent's `data.img` with no quiesce of any kind, by design ("a plain
   filesystem operation that needs no live VM"), while the parent has that ext4
   mounted and dirty. Admission requires `Filesystem state: clean` and no
   `needs_recovery`, which a live-mounted filesystem never satisfies.

   This is the assumption the feature rests on, and it is the one that was asked
   to be validated. Everything around it works. The decision is a data-integrity
   policy: the check conflates journal *recovery* — routine, safe, the reason
   ext4 has a journal, and the designed response to the crash-consistent image a
   `clonefile(2)` of a live file produces — with fsck *repair*, correctly
   forbidden. The recommendation on the issue is to freeze the parent's
   filesystem around the clone **and** permit recovery for a forked disk, keeping
   repair and recorded-error refusal untouched. *The only thing still blocking
   criterion 23.*

3. **There is no reconciliation** (P0). `vz up` refuses every ProjectDefinition
   change before admission -- `project definition drift`. No plan, no durable
   claim, and a mutable change gets the same code as an immutable one. Neither
   of criterion 22's normative sub-documents has an implementation subject:
   `admit_reconcile_round`, `ReconcileInputSnapshot` and `effective_digest` exist
   nowhere in `crates/`. *Blocks criterion 22 entirely.*

4. **Legacy sandbox migration fabricates negotiated capabilities** (P1) for eight
   capabilities, four of which the matrix marks PLANNED. Same false claim as the
   capability fix above, on a different surface -- and it bears on criterion 19's
   "legacy records do not acquire Docker defaults", which PASSES on hardware
   today, so its check does not cover it.

5. **A guest RPC was added without bumping `AGENT_PROTOCOL_REVISION`** (P1). See
   the next section: this one defect produced two false product findings in a
   single night.

6. **No `SecretBinding` exists** (P1) -- not in the project schema, not in
   `vz-runtime-contract`. The gate check is written and waiting.
   *Blocks criterion 18's secrets clause.*

7. **A minted-but-never-booted fork cannot be reclaimed** (P2, `vz-5v8.5`).
   Mechanism now confirmed and it is the same one as the eight above:
   `prepare_delete_absence` has two never-started branches and both are keyed on
   the Environment (`lifecycle_generation == 0`, `prior.generation == 1`). A fork
   is minted at whatever generation its Environment has reached, so neither can
   ever fire for one. The authority it needs already exists —
   `require_machine_admission_fence`, added for the Up path in this same
   situation — so the fix is concrete. Left out of this branch deliberately:
   delete governs reclaiming real resources and wants its own hardware evidence,
   which the Up path's does not provide.

8. **A stuck scoped operation has no supersede path** (P2). It blocks every new
   lifecycle operation on its Environment until replayed with its own request and
   idempotency IDs, which the CLI prints on every run.

## Four findings that were not product defects, and why that matters

Every product bug filed from gate output on 2026-09-09 was misattributed. The
fixes above came from agents who ran things; these came from reading failure
text as diagnosis.

| Filed as | Actually |
|---|---|
| Guests resolve through public DNS | A stale guest bundle ignoring a correct `vz.dns` kernel argument |
| `topology.rs` rubber-stamps capabilities | That file does not exist; the symptom was real, nothing consumed the matrix |
| A stream grant also passes UDP | `busybox nc -u` without `-z` exits 0 whether the port is refused, unbound or live |
| macOS Machine takes no fabric address | It holds its derived address; the probe ran `/bin/busybox` on a guest that has none |

**The gate says which assertion failed. It does not say why, and the exit code
often belongs to a different layer than the message.** The macOS one is the
clearest: `exit 5` was `backend_unavailable` from a probe that never executed,
wearing the label of an address claim.

Two of the four trace to one cause -- `--reuse-guest-bundles`, the DEV shortcut
that ships a guest agent older than the host expects. The builder's own comment
says to rebuild every run "so source changes cannot be silently shipped with a
stale guest executable". The argument is for the shortcut refusing when the
guest surface has moved, not for retiring it.

The other two were probes that could not fail, which is the same defect the
harness has now had three times: `BUSYBOX_SHIM` never setting the `mode` it
branched on, `FAKE_VZ` refusing an unknown `--environment` on one code path
only, and the `nc` shim returning the denial the check wanted rather than what
the real tool does.

## What the gate will not tell you yet

- **The fork measurement has still never completed**, and now for one reason
  rather than a list: everything up to `vz up --fork-from` passes end to end on
  real Machines — parent up (cold, ~32 s), Docker context resolved out of the
  Machine's private config, engine answering on the first poll sample, sentinel
  round-tripped, warm image imported and resolving to a digest, the label rule
  mapping this worktree's branch before any fork exists, the parent's one
  clonable disk identified — and the fork itself is refused by the fabric. Two
  bounds a first complete run will settle: `FORK_SPEEDUP_MIN = 2.0` and
  `FORK_FREE_SPACE_FRACTION = 0.5`.

- **The fork check now reaches the fork against real VMs.** Its setup passes end
  to end: parent Environment up (cold, 34.8 s), identities recorded, sentinel
  round-tripped, warm image imported into the parent's own engine. Getting there
  cost two repairs worth naming. The Docker failure was the harness aiming
  `docker --config` at the lane's directory; the context lives in the Machine's
  own private config, whose path `vz status` reports as `docker_context.config_dir`
  -- it was in the 200-byte slice the original diagnosis was truncated inside.
  And the fork's engine is now waited on with the contract's own declared
  `poll.docker.engine_ready`, because a dockerd still starting reports an *empty*
  image store, which is indistinguishable from a fork that inherited nothing --
  the warm-state claim, decided by a race.

  What has still never completed is a fork measurement against a real parent.
  Two bounds it will settle: `FORK_SPEEDUP_MIN = 2.0` and
  `FORK_FREE_SPACE_FRACTION = 0.5`. The check records the cold Up's wall time and
  free-space delta beside the fork's, so both can be judged from evidence;
  revising them is a product decision, not a repair to the measurement.
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

A free-space measurement on APFS is noisier than the numbers above suggest, and
the fork check failed four times in both directions -- a 32 MiB parent appearing
to cost anywhere from **-82 MB to +280 MB** -- before the noise was understood:
freeing a tree is asynchronous, so a control deleted before the window reclaims
inside it; `f_bavail` sampled without a sync charges earlier writes to whichever
window is open when writeback runs; and a bound of the same order as that
movement decides nothing. The window now excludes the control's deletion, syncs
before sampling, and the bound is half the parent's allocated size -- what the
criterion separates is a clone from a deep copy, and those differ by the parent's
*entire* allocated size, so half clears the noise floor in both directions where
a quarter only bought flakiness.

A fourth measurement lesson, from three flaky assertions fixed in one night:
**volume free space is a global observable, so an absolute bound on it cannot be
asserted anywhere else may be writing.** All three passed run alone and failed
under load — a 32 MiB parent appearing to cost 127 MB while neighbours wrote.
A gate lane may assert it, because the lane owns its volume; a parallel test
suite may not. The Rust test now observes copy-on-write *directly* through
`vz_macos_provision::clone::first_physical_extent`: two files sharing blocks
report the same device offset, one holding its own bytes reports a different one.
That is what free space was only ever a proxy for. Two traps came with it — a
deep-copy control built from `std::fs::copy` is not one, because on macOS it
reaches for `fclonefileat` and reported a cost of **zero**, a control that was
secretly the thing under test; and the driver test asserting EPERM from `killpg`
was encoding a platform assumption that measurement disproved.

The last row is the one that keeps being rediscovered. A per-file allocated-size
comparison reads a *correct* copy-on-write clone as a deep copy. Criterion 23 was
rewritten around it once and the fork check asserts the per-file equality
alongside the free-space bound so it cannot be undone by accident.
