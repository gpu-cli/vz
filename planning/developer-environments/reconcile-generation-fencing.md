# Exact-generation reconciliation fencing

Status: authoritative 0.4 reconciliation design
Issue: `vz-mzs.2.5.5.9`
Release goal: [`GOAL-0.4.0.md`](GOAL-0.4.0.md)

## Purpose

A planned action is authority over the replica state observed when the plan was
created, not authority over whichever generation happens to occupy the same
service name and replica index later. A stale create, recreate, or remove for
generation N must never stop, remove, reserve over, release resources belonging
to, or otherwise alter replacement N+1.

Every scoped production action therefore carries an immutable expected-state
precondition. Planning captures it from authoritative topology, journal,
binding, and observed state. The action hash, persisted session, batch audit,
executor admission, and journal mutations preserve and revalidate the same
identity. Target-only and container-ID-only mutation paths are forbidden.

## Action schema v3

The normative shape is equivalent to:

```rust
enum Action {
    ServiceCreate {
        target: ServiceReplicaKey,
        precondition: ReplicaPrecondition,
    },
    ServiceRecreate {
        target: ServiceReplicaKey,
        precondition: ReplicaPrecondition,
    },
    ServiceRemove {
        target: ServiceReplicaKey,
        precondition: ReplicaPrecondition,
    },
}

struct ReplicaPrecondition {
    workload: MachineWorkloadScope,
    environment_generation: u64,
    journal_head: ExpectedJournalHead,
}

enum ExpectedJournalHead {
    NeverJournaled,
    Exact {
        reservation_id: String,
        service_generation: u64,
        ownership: Option<ContainerGenerationOwnership>,
    },
}
```

These fields cross a persistence boundary and must be constructor-validated.
`MachineWorkloadScope` binds project, Environment, Machine, Machine
incarnation, and stack. `environment_generation` binds the current aggregate
authority. The target binds service and one-based replica index.

`NeverJournaled` means no journal generation has ever existed for the exact
workload and target, and no observed projection exists. It does not mean merely
that no generation is currently active. Consequently a create-and-delete ABA
between planning and execution invalidates an absence precondition.

`Exact` names the latest immutable journal head, including a terminal `Cleaned`
or `Failed` head. `ownership: None` means that exact generation has no runtime
binding. `Some(ownership)` must equal the complete immutable generation binding,
including reservation, container ID, runtime generation, stack, and topology
scope. Journal service generation and runtime generation are distinct values and
both are fenced.

`MachineWorkloadScope` and `environment_generation` name the current authority
under which the successor action is admitted; the persisted `Exact` predecessor
intent retains the authority under which that immutable journal head was
created. Consequently, a `Cleaned` or `Failed` terminal predecessor intent may
name an older Machine incarnation, while project, Environment, Machine, and
stack identity must remain equal to the current workload. In the v4 journal
model `Failed` is strictly unbound, while `Cleaned` retains its immutable
historical binding and `ownership`; admission must prove terminality before
accepting that intent/ownership incarnation mismatch. Any bound nonterminal
predecessor must match the current Machine incarnation exactly.
`NeverJournaled` remains absence across the stable workload target's complete
journal history, not merely absence in the current incarnation.

The full `ContainerGenerationOwnership` belongs in the action. A reservation
token can currently be dereferenced through immutable journal tables, but it
does not make persisted action/audit identity self-contained and does not bind
runtime generation, container ID, or topology scope at the action boundary.
Reservation ID, service generation, and full ownership together are the minimum
unambiguous predecessor proof.

Mutable journal status is not part of the precondition. The same claimed action
must be resumable while its exact intent moves through states such as `Running`,
`CleanupPending`, and `Cleaned`. State changes are instead authorized by the
durable action claim and exact journal CAS operations.

A recreate requires an exact bound predecessor. A terminal `Failed` intent is
strictly unbound, and removing that exact failed intent performs only the
applicable journal CAS with no runtime call. An unbound nonterminal `Intent`
row is not proof that the runtime has no reservation: claimed
execution first inspects the exact reservation. `Absent` permits an exact
journal CAS; an exact reserved generation is bound and cleaned under the claim;
foreign, replacement, legacy, or malformed results fail without mutation. A
persisted `Reserved` row is necessarily bound because binding and the
`Intent`-to-`Reserved` transition are atomic; an unbound `Reserved` row is
malformed and fails closed. A
bound activation failure is represented by the nonterminal `Blocked` state. A
create following an exact bound `Blocked` generation cleans only that generation
before reserving its successor. It must not reinterpret `Failed` as bound or use
the historical-incarnation exception for a nonterminal `Blocked` predecessor.

## Authoritative planning snapshot

The planner first derives candidate action kinds and targets without effects.
StateStore then attaches preconditions from one consistent snapshot containing:

- the stable stack workload owner;
- current Environment generation and Machine incarnation;
- the exact latest journal intent for every candidate target, ordered by
  service generation;
- its immutable generation binding, when present; and
- the exact observed-state projection and its consistency with the journal.

Observed state alone is not planning authority because it does not contain the
journal service generation. A missing or inconsistent owner, intent, binding,
or observed projection is a state conflict, not an absent replica. All
production action constructors, including health-restart planning, use the same
StateStore fence builder. The claim transaction revalidates the snapshot because
planning and execution are separated by a race window.

## Durable claim and exact StateStore CAS

The exact batch audit row is the durable action claim. Store schema v6 adds a
partial unique constraint equivalent to:

```sql
CREATE UNIQUE INDEX reconcile_one_started_replica
ON reconcile_audit_log(stack_name, service_name, replica_index)
WHERE status = 'started';
```

Within one `BEGIN IMMEDIATE` transaction, `start_reconcile_batch` must:

1. validate the persisted session, cursor, exact action slice, and full hashes;
2. reject duplicate replica targets in the batch;
3. revalidate every v3 precondition against current topology, latest journal
   head, binding, and observed projection; and
4. insert every `started` audit row.

Any stale fence, existing foreign claim, or insertion failure rolls back the
whole batch. An idempotent replay is accepted only for the same session, action
index, and action hash. The resulting execution key contains `session_id`,
`operation_id`, absolute action index, and action hash. Sessions and their action
JSON/hash are immutable and non-deletable while audit rows refer to them;
otherwise the audit must store the full action payload itself. A new apply may
supersede an effect-free active session, but it must not supersede a session with
a `started` claim.

All planned journal writes use claim-qualified APIs equivalent to:

```rust
require_started_action_claim(claim, action)
inspect_claimed_predecessor(claim)
begin_claimed_predecessor_cleanup(claim, now)
complete_claimed_predecessor_cleanup(claim, now)
resolve_or_begin_claimed_successor(claim, runtime_input, exact_allocation, now)
bind_claimed_successor_generation(claim, binding)
publish_claimed_successor_success(claim, reservation, ready, now)
```

Before resolving a fresh create or recreate successor, the executor derives the
exact requested container ID and asks StateStore for a deterministic,
claim-qualified reservation preview. The preview performs full claim
validation, writes nothing, and uses a versioned digest of the complete
execution key, topology authority, target, generation, and requested ID. The
executor inspects that exact runtime reservation during whole-batch preflight;
anything other than `Absent` is a conflict unless StateStore has already proven
that it is this claim's linked successor. Successor resolution must return the
same previewed identity exactly.

The predecessor inspection result is safe by construction. An exact unbound
`Intent` or `Blocked` result explicitly requires inspection of that exact
runtime reservation before any journal mutation. An exact unbound terminal
`Failed` result explicitly forbids a runtime call. Bound and never-journaled
results are distinct variants. Bound inspection further separates a generation
that needs cleanup, a `CleanupPending` crash-replay generation, and historical
terminal `Cleaned` evidence for which a runtime call is forbidden. Callers do
not infer these obligations by interpreting a generic status value.

`begin_claimed_predecessor_cleanup` loads the named reservation directly. It
requires that reservation and service generation to remain the latest head,
requires the binding to equal the action's complete ownership, validates the
observed projection and workload authority, and CAS-transitions only that intent
to cleanup. Same-claim replay may continue its own exact cleanup. A foreign or
later head is always a state conflict.

`resolve_or_begin_claimed_successor` initially requires the exact planned head or
true `NeverJournaled` state. After claimed exact cleanup, it permits the planned
head in `Cleaned` and reserves precisely its successor generation. Replay may
adopt that successor only when its journal action digest binds the same execution
key and action hash. It never adopts an arbitrary active generation.

A bound successor activation failure is first published as nonterminal
`Blocked`. Separate claim-qualified successor cleanup begin and complete calls
perform `Blocked -> CleanupPending -> Cleaned` around the exact runtime cleanup.
No failure publication may assert that runtime cleanup has begun, and replay
accepts only the same durable evidence rather than overwriting its reason.

Allocator mutation follows the same authority boundary. Create and recreate
persist only the claimed target's port, network, and derived service mount-tag
allocation in the same transaction that reserves the claim-linked successor
intent. The predecessor must already be terminal; replay requires both the
linked successor and exact allocation bytes, and never overwrites them with new
input. Every sibling lease and mount offset is preserved. Remove atomically
revalidates its started claim, exact terminal predecessor, and absence of a
successor in the same transaction that removes only that target's leases.
Replaying after commit is an idempotent already-released result. A transaction-
time authorization token is not mutation authority. Whole-stack allocator
replacement and every raw journal mutator reject while a relevant started
claim exists; recovery receives no exemption.

User-planned mutation code must not scan current records by target and then act
on the first match. Recovery may enumerate journal records, but it is a separate
path and may clean only each record's exact immutable binding; recovery never
turns a stale user plan into authority over a replacement.

## Strict mutation ordering

The required order is:

1. validate definitions and capture the immutable operation input snapshot;
2. derive the pure plan and stage its immutable scoped manifest before creating
   a session or acquiring a claim; a staging failure leaves no active session;
3. persist the exact session/progress identity;
4. atomically revalidate all action fences and acquire all batch claims;
5. revalidate every claim and current authority, validate all successor inputs,
   derive every deterministic reservation preview, and perform the complete
   batch of read-only exact runtime inspections;
6. only when every action passes may claimed predecessor cleanup begin; if any
   action fails preflight, every predecessor and successor remains untouched;
7. only then create or update sandbox state, activation manifests, staged
   secrets, volumes, network/port allocations, guest state, or runtime state;
8. for recreate or create with a bound predecessor, admit and complete exact
   generation cleanup before releasing/reassigning ports or preparing the new
   create; and
9. reserve, bind, activate, and publish the claimed successor. Remove releases
   resources only after exact cleanup and a final no-successor check.

The immutable operation input manifest defined by `vz-mzs.2.5.5.8` is the only
pre-claim filesystem write: it is inert host-side planning metadata, is not
installed into an Environment or Machine, and must be atomically associated
with or garbage-collected alongside the session. All activation and
target-affecting filesystem writes remain after the claim.

Database admission prevents cooperating reconcilers from replacing the target
during execution. Runtime inspection and exact-generation stop/remove remain
the final fence against an out-of-band runtime replacement. Any replacement
result fails closed without touching the replacement.

Terminal replay never reconstructs a result by re-reading mutable activation
inputs, allocator state, bindings, or the runtime. A completed or failed
session is replayable only when its immutable session identity, complete action
plan, terminal cursor/status, and one terminal audit row for every exact action
form a verified bijection. The executor reconstructs the original ordered
outcomes from those audits and performs zero external or database mutation.
Missing, duplicate, nonterminal, mismatched, or tampered audit evidence is a
state conflict rather than permission to execute again.

## Stack teardown finalizer authority

Stack teardown contains effects outside individual replica removal: persisting
the empty desired state, shutting down stack-wide runtime resources, optionally
removing volumes, and publishing the terminal event and receipt. Those effects
must not run after the remove claims have been released, because a concurrent
apply could publish a replacement generation in the gap.

A non-dry teardown with replica removals therefore uses a reserved durable
operation purpose and a typed two-phase executor API:

1. acquire the exact remove claims and execute the remove-only action batch;
2. return one opaque pending-finalizer authority while the session and all
   claims remain active;
3. while holding that authority, persist empty desired state, shut down the
   stack runtime, and perform the requested volume cleanup; and
4. claim-qualify the terminal batch commit, then atomically persist the
   terminal event and request-bound receipt.

Generic claimed execution, generic orchestrator resume, and the generic
StateStore batch commit reject the reserved teardown purpose. The privileged
StateStore commit is crate-private, accepts the pending token's exact claims,
and transactionally revalidates the remove-only action/claim bijection before
releasing the fence. A failed remove batch is committed by the typed executor
without granting broad-finalizer authority. A non-dry teardown with no replica
actions fails closed until a durable stack-level finalizer claim exists.

The daemon serializes teardown finalizers per stack from the first persisted
identity/receipt check through final receipt publication. This makes the opaque
pending authority single-consumer within the one daemon process that owns the
state store: an identical concurrent retry waits, then observes the first
terminal receipt instead of repeating broad effects after the generation fence
has been released. Process loss releases the in-memory serialization guard but
not the durable session/claims. A terminal session without its final receipt
fails closed; exact crash-window receipt replay and stable effect counters are
completed under `vz-mzs.2.5.5.11` rather than by rerunning unfenced effects.

The teardown request identity binds the stack, complete workload scope,
request ID, dry-run bit, and volume-removal bit. Active-session retry and
terminal receipt replay validate that same digest. Reusing a completed request
ID with different options, including switching to dry-run, is a zero-effect
state conflict.

## Persistence, hashing, and audit

Action serialization is schema version 3 in sessions, progress, scoped
manifests, and replay. The action hash uses a new domain/version and
length-framed canonical encoding of action kind, target, workload scope,
Environment generation, expected-head variant, reservation, service generation,
and every ownership field. Field order, optional values, and variant tags are
explicit; decorated-name concatenation is not an encoding.

The scoped executor identity also includes the full single-action hash and
session ID. Journal `action_digest` binds the execution key and action hash, so
a crash replay can distinguish its own successor from a foreign generation. A
successor's canonical journal digest additionally binds the full normalized
runtime-create input and exact normalized allocator result, so replay cannot
silently reuse a changed manifest, secret, mount, network, or port assignment.
Audit identity may retain only the action hash when the referenced v3 session is
immutable and retained. Loading a session, progress record, scoped manifest, or
audit re-computes and verifies the full hash before admission.

Every control-plane status projection carries replica identity as the tuple
`(service_name, replica_index)`, where the index is one-based and zero is
invalid. `service_name` remains the undecorated Compose service key so config,
port, and dependency lookup cannot alias it with a name that happens to end in
digits. A decorated display name is presentation-only and is never decoded back
into authority. In particular, `(api, 2)` and `(api-2, 1)` remain distinct over
gRPC, the HTTP bridge, and CLI reconstruction. Older wire payloads that omit the
replica index fail closed instead of silently acquiring replica-one authority.

## Store v5 to v6 migration

Store schema v6 rebuilds the session and progress tables because v5 constrains
`action_schema_version` to 2. No v2 action can be upgraded into a v3 wildcard or
have a predecessor inferred at migration time.

Migration performs a complete preflight and moves terminal v2 sessions and
audits to explicit archival quarantine. Any active v2 session, pending progress
record, or `started` audit row makes migration fail with zero writes; explicit
recovery or quarantine is required before retrying. Such rows are never resumed
or synthesized as v3 actions. Exact journal/runtime ownership remains
authoritative during that recovery, after which a new v3 plan is required. If
the terminal archival set cannot be quarantined atomically, migration aborts and
leaves v5 byte-for-byte usable.

Legacy scoped action manifests are never interpreted as unconstrained actions.
Resume rejects them and requires a new plan. Migration failpoints must roll back
the entire schema/data change, and reopen must preserve and revalidate the same
v3 action and claim.

## Store v6 to v7 claim-authority migration

Store v6 is the published Action-v3 persistence foundation. Claim-authority
hardening therefore advances the store to schema v7 rather than silently
changing canonical v6 DDL. V7 makes immutable session identity, action JSON/hash,
plan length, and start time non-updateable; it likewise makes every audit claim's
session, action index/kind/target/hash, and start time non-updateable and retains
claim rows. Only session cursor/status/completion fields and audit
status/completion/error fields may follow their validated state machines.

Migration validates every v6 session, progress record, and audit against its
strict Action-v3 payload and hashes before installing the guards. A v6 `started`
audit is not trusted as a v7 claim because it predates atomic claim-time
revalidation; its presence makes migration fail with zero writes until explicit
recovery resolves it. Effect-free active sessions may migrate and must acquire a
fresh v7 claim before mutation. Terminal history is preserved under the new
retention rules. Migration failpoints roll back triggers, version, and all rows,
and reopen validates the exact canonical v7 trigger definitions.

## Required acceptance tests

The implementation is incomplete until deterministic tests establish all of
the following:

1. Connection A plans recreate/remove for N; connection B cleans N and publishes
   N+1; A fails at claim and at executor revalidation with zero runtime,
   allocator, network, volume, sandbox, secret, or activation-manifest mutation.
   N+1 intent, binding, observed state, and resources remain byte-identical.
2. Two connections use a barrier to claim the same planned N from distinct
   sessions. Exactly one acquires the `started` claim, the loser gets a state
   conflict, and exact runtime cleanup is invoked once. Supersession cannot
   invalidate the winner.
3. Table-driven mismatches in reservation, service generation, runtime
   generation, container ID, project, Environment, Machine, stack, Machine
   incarnation, and missing binding all fail before effects. Database snapshots,
   SQLite change counts, and external mutation counters remain unchanged.
4. A stale `NeverJournaled` create fails after either a concurrent create or a
   create-and-clean ABA.
5. Crash/reopen resumes only the same claim after: claim-before-runtime,
   `CleanupPending` after runtime removal, predecessor-cleaned before successor
   intent, and successor-reserved before activation. A foreign claim is denied.
6. A runtime replacement between database admission/inspection and exact cleanup
   returns replacement, leaves it untouched, and does not publish false cleanup.
7. Tampering with any persisted precondition, action hash, session/audit link,
   journal action digest, or scoped manifest is detected before effects.
8. V5 active-state preflight refusal with zero writes, terminal archival
   quarantine, legacy manifest rejection, injected migration rollback, and v3
   reopen/replay pass.
9. `(api, replica 2)` and `(api-2, replica 1)` remain distinct; removal of an
   exact unbound failed head makes no runtime call; stale failures preserve all
   port and network ownership.
10. Generic executor, orchestrator-resume, and StateStore commit paths cannot
    terminalize a pending teardown. A wrong, missing, reordered, or foreign
    claim and a cleanup executor for another stack all fail without releasing
    the active fence.
11. Two concurrent identical teardown requests cannot both consume broad
    finalizer authority. The delayed request observes the first receipt and
    cannot shut down or remove resources from a replacement admitted after the
    first teardown commits. Changing any request-bound option conflicts before
    mutation, and a non-dry zero-action teardown performs no broad effect.

These tests supplement the host×Machine-target and aggregate E2E requirements
in the 0.4 goal. They do not replace release-built local-Mac evidence.

## Relationship to adjacent work

- `vz-mzs.2.5.5.8` owns the immutable effective desired-service input snapshot,
  including referenced definitions and secret content digests. That desired
  identity and this predecessor precondition are sibling inputs to action
  persistence and hashing; neither substitutes for the other.
- `vz-mzs.2.5.5.9` owns plan-time predecessor identity, durable action claims,
  exact cleanup/create CAS, and the no-effect-before-admission ordering in this
  document.
- `vz-mzs.2.5.5.10` owns generation-qualified live lifecycle observation, exit
  receipts, health exec authority, cancellation, and timeouts. This design does
  not define exit or exec semantics. It consumes exact ownership inspection and
  exact-generation stop/remove as its final runtime safety boundary.
