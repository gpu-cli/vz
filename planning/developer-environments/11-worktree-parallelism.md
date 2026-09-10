# Machine forking for parallel worktrees

Status: accepted for 0.4.0 (criterion 23)
Parent: [the product contract](../../docs/developer-environments.md),
[the 0.4 gate](GOAL-0.4.0.md)
Related: [01-environment-lifecycle.md](01-environment-lifecycle.md),
[04-isolation-storage-network.md](04-isolation-storage-network.md)

## The problem

A developer running several agents in parallel keeps one git worktree per agent
and wants each to have its own running code. The obvious approach -- one
Environment per worktree -- multiplies every Machine in the topology, and the
Machine you most want to replicate is often the one you least can.

Two constraints shape the design, and they point in different directions:

1. **macOS guests are licence-limited to two per host.** Where the topology
   includes a native macOS Machine, per-worktree replication of it is
   impossible, not merely expensive.
2. **Linux Machines replicate freely but duplicate content.** Each carries its
   own Docker engine, so N Machines pull the same base images N times and
   rebuild the same dependency layers N times.

## The primitive: fork a Machine within its Environment

**One Environment per project. Machines are forked inside it, not replicated
alongside it.**

A fork is a new Machine seeded from an existing Machine's disk, in the same
Environment, with its own identity. Everything else follows from identity:

- **Addressing is already correct.** `assign_host_offset` derives from
  `[environment_id, network_id, attachment_id]`, so a fork's new `machine_id`
  yields a new attachment and therefore a different address on the same subnet.
  Forks are siblings on one fabric, reachable from each other and from every
  shared Machine. No allocator, no collision, nothing to add.
- **Docker follows the disk.** The engine, containerd, BuildKit state and image
  store are guest-side files, so a fork inherits its parent's warm cache. The
  host side -- context name, relay socket, `engine_id` -- is re-minted from the
  new identity, exactly as it is for any new Machine.
- **Isolation is unchanged.** A fork is a Machine in one Environment. Nothing
  here opens a path between Environments.

## Why fork rather than N declared Machines

Three separate problems, one primitive:

**Freshness.** An agent's loop is try, fail, try again, and a failed attempt
contaminates its Machine -- a half-applied migration, a poisoned build cache,
stray containers. Without a fork the agent either continues in a dirty
environment and produces untrustworthy results, or tears down and pays a cold
start. With a fork, rollback is not an undo; it is a delete. This is the
correctness argument, and it is the strongest one: a test run against a
contaminated Machine tells you nothing.

**Duplication.** A typical image is both pulled and built:

```dockerfile
FROM rust:1.x            # pulled -- identical across worktrees
COPY Cargo.toml Cargo.lock
RUN cargo build --deps   # built  -- identical until dependencies change
COPY src/
RUN cargo build          # built  -- the only layer that differs
```

Only the last layer genuinely differs between worktrees. Three independently
declared Machines each pull and rebuild everything above it. A fork inherits all
of it and diverges only where the code does. A BuildKit remote cache addresses
the built half and needs per-project configuration; a fork addresses both
halves and needs none, because it does not distinguish them.

**Cold start.** The dependency layer, the warm image store and a seeded database
are minutes each. Forking pays them once per baseline rather than once per
worktree.

## Scope: which Machines are forked

Linux-parallel is the primary case. Where the topology is Linux only -- as it is
once a project is ported -- every Machine is forkable and the design is simply
one fork per worktree.

Where a native macOS Machine is present, it is capped and is therefore shared,
with worktrees projected into it rather than replicated with it. Its freshness
need is already met without forking: `vz up` creates macOS Machines as private
clones of a registered template, so a fresh macOS Machine is a delete followed
by an up, with no install or patch work. Fork is not required there.

A Machine holds at most one workspace projection (`workspace:
Option<WorkspaceProjection>`), so "several worktrees inside one Machine" is not
expressible today and is deliberately out of scope: three compose stacks in one
Docker engine collide on container names and published ports, which is the
problem Machines exist to avoid.

## What the implementation found

Recorded here because two of them contradict what this document assumed.

- **The default label is a published rule, not a lookup.** Every character
  outside `[A-Za-z0-9._-]` becomes `-`, runs collapse, truncate at 64. So
  `feat/third-environment` addresses as `backend@feat-third-environment`, and an
  agent can compute the address before the fork exists rather than having to
  read it back.
- **`vz exec --machine backend@feat-x` needed no change.** A fork's
  `MachineInstance.name` IS `backend@feat-x`, so the existing exact-name match
  resolves it and `UNIQUE(environment_id, name)` makes it unambiguous by
  construction. The `@` is spelling, not a second lookup dimension.
- **Only one file is copied.** A Developer Linux Machine has no root disk -- it
  boots kernel plus initramfs -- so the warm state IS the Docker data disk, at
  `<store>/data/docker-machines/<sha256(stack_id)>/data.img`. And because
  `stack_id` derives from `(project, environment, machine_id)`, the fork's disk
  lands at its own path with no rename step.
- **"`delete` already provides the discard half" was wrong**, and not by a
  wiring gap. Every teardown primitive is fenced on a persisted
  `EnvironmentLifecycleOperation` whose structure check requires one machine
  step per Machine in the Environment, so a fork-scoped Delete is refused by
  that invariant. Reclaiming one fork needs a machine-scoped lifecycle
  operation, which is real work in `vz-stack` and is tracked separately. Until
  it lands `vz delete --machine` refuses rather than half-reclaiming: a partial
  teardown leaks a host Docker context and a runtime store while the ownership
  rows claim reclamation, which is the exact unaccounted state the ownership
  design exists to prevent.
- **The directory-tree clone already existed.** `clone_path` in
  `vz-macos-provision` has wrapped `clonefile(2)` for trees, with tests for
  recursion, symlinks and inode separation, since before this work started. The
  `OwnedResourceKind::MachineFork` variant was still needed, but for a different
  reason than assumed: it marks a Machine as runtime-minted rather than
  declared, which is what exempts it from reconciliation.
- **Fork of a fork is refused**, so one label is one address; forking is
  Linux-only, because macOS Machines are shared by design; and the parent must
  have booted once, because seeding needs its Docker disk to exist.

## Naming, so an agent can target an instance

Forks are addressed as `<machine>@<label>`, where the label is caller-supplied
at fork time and defaults to the checked-out branch of the worktree that created
it.

- `vz exec --machine backend@feat-x` reaches that fork explicitly, from anywhere.
- `vz exec --machine backend` reaches the DECLARED Machine called `backend`, not
  a fork of it -- see below.
- `vz status` lists forks with their labels and their parents, so an agent can
  discover what exists.

**A bare `--machine backend` does not resolve to the current worktree's fork.**
This document originally claimed it did. It does not, and it cannot without a
change nobody has argued for: the resolver matches an exact machine id or an
exact name, there is no worktree dimension in it, and `backend` is the exact
name of the declared Machine. So a bare selector reaches the parent. An agent
addressing a fork names the fork.

That is the better behaviour anyway. A selector whose meaning depends on which
directory the caller happens to be in is precisely what an agent driving many
worktrees cannot reason about, and the label rule already lets it compute the
full address before the fork exists.

Ambiguity therefore never arises from a fork's name: names are unique within an
Environment, so `--machine <anything>` resolves to exactly one Machine or to
none. What does fail closed is selection with NO `--machine` when more than one
Machine exists, and it lists every candidate with its identity -- which is what
an agent needs in order to correct itself. Labels are caller-supplied rather
than ordinal because ordinals shift as forks come and go, and an agent must be
able to predict the name it will target.

## CLI surface

The public CLI has five lifecycle verbs and richer topology operations belong to
typed APIs, but the agent speaks the CLI. Fork is therefore a flag on `up`
rather than a sixth verb:

```
vz up     --fork-from backend --as backend@feat-x
vz exec   --machine backend@feat-x -- cargo test
vz delete --machine backend@feat-x
```

The discard half is NOT free: see the correction above. `vz delete --machine`
resolves a fork and then refuses, because every runtime teardown primitive is
fenced on an Environment-wide lifecycle operation. Reclaiming one fork needs a
machine-scoped lifecycle operation, which is tracked separately and which
criterion 23 cannot pass without.

## Performance is part of the contract, not an afterthought

A fork that costs as much as a cold boot has no value, so an unmeasured fork
proves nothing. The gate criterion pins measured bounds:

- a fork of a warm Machine reaches ready **substantially faster than a cold `up`
  of the same definition**, with both measured in the same run so the comparison
  is not against a remembered number;
- the fork's Docker image store **contains its parent's images without pulling**,
  proven by image digests present and a pull count of zero;
- forking **does not deep-copy the disk** -- the volume's free space falls by a
  small fraction of the parent's allocated size, which is what makes
  copy-on-write observable rather than assumed. Free space, not per-file
  allocated size; the measurement below says why.

### Measured, 2026-09-09

The third property was the design's load-bearing assumption. It holds.

A real native macOS Machine template disk -- 80 GiB logical, **32.9 GiB actually
allocated** -- cloned with `clonefile(2)`:

| | |
|---|---|
| wall time | **0.029 s** |
| volume free-space delta | **28 KB** |

And a file held open by a process writing and `fsync`ing continuously (10,288
writes completed before the call) cloned in **0.077 s**, exit 0. So a fork does
not require the parent stopped: `clonefile` operates on the path and does not
contend with an open writer.

Two findings that change how the criterion must be written:

- **Measure volume free space, not per-file allocated size.** APFS reports both
  inodes as fully allocated -- the clone's `st_blocks` matched the parent's
  32.9 GiB exactly -- because they reference the same blocks. A check comparing
  per-file allocated size would read a perfect copy-on-write clone as a deep
  copy and fail. Free-space delta is the observable that distinguishes them.
- **A clone of a running Machine is crash-consistent, not application-
  consistent.** It is the state a power cut would leave: the guest's ext4
  journal replays on mount, but a write in flight when the clone was taken may
  be partial. The fork should therefore ask the guest agent to sync its
  filesystems immediately before the clone -- quiescing without stopping -- and
  the criterion should say which of the two consistencies it proves.

## Endpoints: a fork publishes none

The design was silent here and it cannot stay silent. Endpoint, host-export and
host-import names are `UNIQUE(environment_id, name)`, and a host export owns a
host port. Two forks of `backend` cannot both publish `api` on 8080, so either
forking mints new names -- which an agent then cannot predict -- or it mints
none.

**A fork mints no endpoints, no host exports and no host imports.** It is
reachable at its own derived fabric address and addressable as
`<machine>@<label>` through `exec`; it does not answer the parent's declared
names. This is the same reasoning the section above already applies to three
Compose stacks in one Docker engine, carried to its conclusion: the declared
names belong to the declared topology, and a runtime object does not get to
claim them.

The consequence to accept is that a fork is for work an agent drives -- build,
test, exec -- rather than for serving the Environment's public-like ingress. A
worktree that needs its own ingress needs its own Environment.

## Ownership and reconciliation

- **Forks are owned resources.** `OwnedResourceKind` carries them so Delete
  traverses them. A fork that cannot be fully reclaimed is a leak per attempt,
  and agents generate attempts continuously. Volumes received this treatment in
  criterion 17 and forks need the same.
- **The parent must be provably unaffected**, in the shape criterion 11 already
  uses for a neighbouring Environment: it keeps serving, keeps its identities,
  and returns its sentinel bytes unchanged.
- **Reconcile must not prune forks.** A fork is not in the definition, so `vz up`
  sees Machines the definition does not declare. This is the same question
  criterion 22 asks about reconciliation and must be answered once for both:
  forks are runtime objects, reconcile leaves them, and only `delete` removes
  them.

## Open questions

1. ~~**Can a running Machine's disk be cloned?**~~ ~~**What remains is the
   narrower question of quiescing the guest before the clone.**~~ **Both
   answered.** The clone is milliseconds and kilobytes and does not stop the
   parent, and the fork now runs `/bin/sync` in the parent guest through the
   agent channel immediately before cloning, bounded at 30 seconds, so the fork
   is application-consistent. A parent that is not running has nothing in
   flight; a sync that fails proceeds crash-consistent and says so rather than
   hanging.
2. **What seeds the first Machine of a project?** Cloning a running sibling is
   implicit and surprising; a declared baseline is explicit but needs a way to
   promote a warmed Machine, which is imperative and wants a verb the CLI
   contract does not have.
3. **Two worktrees, two `vz.json` revisions.** Both reconcile one Environment
   from different definitions. The binding probably has to record which revision
   it last reconciled.
4. **Whether `shared_cache` should carry the Cargo registry by default.** It
   would remove the largest remaining duplication for Rust projects, but a
   default that silently shares state across forks weakens the isolation forks
   are for.
