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
  finished coming up** (P0, found the first time the fork check reached real
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

1. **There is no reconciliation** (P0). `vz up` refuses every ProjectDefinition
   change before admission -- `project definition drift`. No plan, no durable
   claim, and a mutable change gets the same code as an immutable one. Neither
   of criterion 22's normative sub-documents has an implementation subject:
   `admit_reconcile_round`, `ReconcileInputSnapshot` and `effective_digest` exist
   nowhere in `crates/`. *Blocks criterion 22 entirely.*

2. **Legacy sandbox migration fabricates negotiated capabilities** (P1) for eight
   capabilities, four of which the matrix marks PLANNED. Same false claim as the
   capability fix above, on a different surface -- and it bears on criterion 19's
   "legacy records do not acquire Docker defaults", which PASSES on hardware
   today, so its check does not cover it.

3. **A guest RPC was added without bumping `AGENT_PROTOCOL_REVISION`** (P1). See
   the next section: this one defect produced two false product findings in a
   single night.

4. **No `SecretBinding` exists** (P1) -- not in the project schema, not in
   `vz-runtime-contract`. The gate check is written and waiting.
   *Blocks criterion 18's secrets clause.*

5. **A minted-but-never-booted fork cannot be reclaimed** (P2), because its
   ownership lacks the two runtime reservations Up takes. Consistent with the
   Environment path's behaviour for a partially-provisioned aggregate, so it was
   left consistent rather than made laxer -- but it is real if
   `vz up --fork-from` ever fails between minting and reserving.

6. **A stuck scoped operation has no supersede path** (P2). It blocks every new
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

The last row is the one that keeps being rediscovered. A per-file allocated-size
comparison reads a *correct* copy-on-write clone as a deep copy. Criterion 23 was
rewritten around it once and the fork check asserts the per-file equality
alongside the free-space bound so it cannot be undone by accident.
