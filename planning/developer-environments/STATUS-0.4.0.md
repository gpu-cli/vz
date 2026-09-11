# vz 0.4.0: where it stands

Status: living summary, rewritten when a gate run changes it
Last measured: 2026-09-10, release candidate `0.4.0-rc19`, Apple-silicon macOS 26.3.1

This is the short answer to "what's left". The gate itself is
[GOAL-0.4.0.md](GOAL-0.4.0.md); this file says which of its claims are proved,
which are disproved, and what is in the way.

## The one-line version

**The topology lane passes.** Ten of ten scenario rows, seventeen of seventeen
sub-checks, `leaks [] cleanup_errors []`, on candidate `0.4.0-rc22`, run
`vz-final-1789083622`. It is an all-or-nothing lane, so every one of those rows was MISSING
before this run.

## The run

```
topology lane clean-provision: outcome=passed  reason=None
sub-checks PASS=17   FAIL=[]   not_implemented=[]

PASS  gate.cli.legacy_removal_and_bootstrap
PASS  gate.cli_api.agreement
PASS  gate.fork.machine_fork_for_parallel_worktrees
PASS  gate.host.import_export_boundaries
PASS  gate.instances.three_concurrent_no_collision
PASS  gate.machines.mixed_profile_topology_status
PASS  gate.migration.install_upgrade_rollback_uninstall
PASS  gate.network.private_topology_paths
PASS  gate.network.public_like_ingress
PASS  gate.storage.workspace_projection_policy
```

## What it took, 2026-09-10

Four product defects, every one of them found by running the installed binary
against real Machines rather than by reading code.

* **`egress: offline` was declared and never enforced.** Measured from inside
  a Machine whose definition said nothing about networking, and which
  therefore took the default: `eth0` on Apple's NAT, public DNS answering, TCP
  to `1.1.1.1:443` REACHED, `example.com` fetched. Up refused every OTHER
  policy as unimplemented, which is exactly why nobody noticed this one was
  unimplemented too -- nothing was ever admitted that could contradict it.
  This single fix unblocked criteria 6 and 7.
* **A first `vz up` stranded its own project** whenever the definition
  declared `machine.workspace`. The slot was reserved after
  `begin_environment_lifecycle`, which moves a first Up out of `Creating` --
  and the reservation requires `Creating`, while every aggregate read refuses
  a declared slot left unresolved in any later state. Criterion 17.
* **A Machine could reach `ready` with no fabric NIC at all.** One unwaited
  `/sys/class/net` scan against an asynchronously-probed virtio-net device,
  with reboot-only configuration, so "not there yet" became "never". It
  reproduces only under load, which is why `--only` passed twice and the full
  lane failed twice. Criterion 5.
* **Criterion 20's `tcp` probe sent plaintext HTTP to port 443** and recorded
  reachable hosts as denied. Every `deny` cell had been passing for the wrong
  reason; the only cell that could expose it was the `allow` cell, which could
  not be built until `EgressPolicy::Allowed` was admitted the same day.

Two checks were also grading themselves against claims the gate does not make:
criterion 6 against required-implementation item 6 rather than acceptance
criterion 6, and criterion 23 against a speed-up bound that no implementation
could meet, because a fork's baseline populates no image store.

And the hardware sleep/wake gate was removed, 1,356 lines: a release gate that
cannot finish without a human at the console is not a gate, and it held
criteria 18 and 20 behind a step neither of them tests. Removing it is what
made `post-wake` runnable, which is how the probe defect above was found.

## The macOS template

Criteria 2 and 5 needed a registered native macOS template, and the one this
host had recorded was gone. Re-provisioning it cost three diagnoses worth
writing down:

1. `vz-macos-setup` failed at "Verifying and caching prepared image" with a
   bare `Error: No such file or directory`.
2. `--native-bundle` wants the CONTENT-ADDRESSED `macos-local/images/<sha>/`,
   not `macos-local/cache/templates/<sha>/` with the recognisable filenames --
   which is the one a reader finds first.
3. The gate requires the `xcode` channel; the first template was `clean`.

The crossing it unlocked is genuinely served, not merely declared: the macOS
Machine holds `10.184.42.13` on the Environment's fabric, and a Linux sibling
reads the path it serves.

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

0. **The native-macos lane's one row.** The capability is proven -- the macOS
   Machine that just passed criteria 2 and 5 served a declared path to a Linux
   sibling -- but that lane's own run has not been executed. It is the only
   one of the 85 rows with no measurement at all.

   Note what provisioning its template requires: `vz-macos-setup` blocks on an
   INTERACTIVE ADMIN AUTHORIZATION. That gives this lane exactly the property
   the hardware sleep/wake gate had and was removed for. Worth deciding
   deliberately rather than inheriting.

1. **A destination policy for `allowed` egress** (`vz-8cq`, in progress). The
   two policies the project schema spells are now enforced and distinguishable
   -- `offline` attaches no external NIC, `allowed` attaches Apple's user-mode
   NAT -- and that is what unblocked criteria 6 and 7. What is not built is a
   policy over WHICH hosts an `allowed` Machine may reach. Apple's NAT is
   unrestricted outbound, and the CIDR and domain policies criterion 6's
   required-implementation item names have no spelling in the schema, so
   criterion 20's matrix records 24 of its 118 cells unexercised in the
   schema's own words. Switch-side NAT with a destination policy is the next
   increment; the design is
   [NETWORK-INCREMENT-PLAN.md](NETWORK-INCREMENT-PLAN.md) step 5's second half.

2. **Under full-lane load a Machine reaches ready with NO fabric NIC**
   (P0, `vz-9x6`). This is now the only thing between the lane and a pass.
   Isolated across four runs on one host:

   | Criterion 5 | Candidate | Result |
   |---|---|---|
   | full lane | rc19 | FAIL |
   | full lane | rc19 | FAIL |
   | `--only` | rc19 | PASS |
   | `--only` | rc18 (older guest bundle) | PASS |

   Not the candidate, not the guest bundle (rc18 and rc19 share a
   `sha256_vmlinux` and differ only in initramfs), not the egress change
   (present in both). It is load: the same check passes alone and fails twice
   running when it comes after nine other sub-checks.

   The failing run's guest had the derived address on the cmdline and no NIC to
   put it on -- `interfaces [{'name': 'docker0', ...}]` and nothing else.
   `docker0` is created late by the Docker engine, so this is not an early
   read: the Machine had reached ready and started Docker, and the only network
   its definition declared still did not exist.

   Egress enforcement did not cause this; it removed what was hiding it. With a
   NAT NIC always present, a missing fabric NIC left `eth0` there and the check
   reported an address MISMATCH; now the fabric NIC is the Machine's only one,
   so the same failure reports as an empty list.

   **Root-caused and fixed** (`f7e2b546`). `configure_fabric_nic` in
   `linux/initramfs/init` scanned `/sys/class/net` exactly once for the MAC the
   host put on the cmdline. virtio-net devices are probed asynchronously, so a
   scan that runs first finds nothing -- and the block's own header says
   configuration is reboot-only, so nothing is permanent. Load is what moves a
   probe past a single unwaited scan, which is why `--only` passed twice and
   the full lane failed twice.

   The scan is now a bounded wait, checked before each sleep so the budget
   bounds the wait rather than exceeding it by one; the first scan runs before
   any sleep, so a healthy boot pays nothing. Two tests against the block
   lifted verbatim from the guest init by the existing harness -- one makes the
   race deterministic and was verified to fail against the old code by setting
   the budget to 0, the other pins the bound. Hardware verification is a FULL
   lane run against `0.4.0-rc20`, because the full lane is where it reproduced
   and `--only` never did.

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

**It happened a third time on 2026-09-10**, and cost a full diagnosis before
being recognised. A criterion 7 run against rc18 failed at `hb-granted: vz
--json up exit 2` with *"host import grants were not accepted by the guest
agent: status: Unimplemented"*, which reads exactly like the egress change
having broken the import relay. It had not: rc17 and rc18 reused a developer
bundle built three and a half hours BEFORE the relay's guest half merged. The
bundle still reports `agent_protocol_revision` 10, so the version gate does not
catch it -- the RPC simply is not there. Criteria 17 and 23 passed on the same
bundles because neither declares a host import, so criterion 7 was the first
check able to see the staleness. **A revision that does not move when a guest
RPC is added makes `--reuse-guest-bundles` silently wrong**, which is item 5 of
"Still open" and now has a second incident behind it.

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
