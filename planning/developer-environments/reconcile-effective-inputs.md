# Immutable effective reconciliation inputs

Status: authoritative 0.4 reconciliation design
Issue: `vz-mzs.2.5.5.8`
Release goal: [`GOAL-0.4.0.md`](GOAL-0.4.0.md)

## Purpose

A reconcile decision and the runtime activation it authorizes must consume the
same immutable inputs. Hashing `ServiceSpec` alone is insufficient: a service
also consumes referenced network and volume definitions plus the exact bytes of
its referenced secrets. Those values can change between planning and execution,
after a crash, or without changing the service object at all.

Before final action planning, scoped production reconciliation therefore
captures a durable, operation-owned effective-input snapshot. Planning,
persisted session identity, action audit, journal `applied_config_digest`, and
execution all refer to that one snapshot. Reopening an operation never rereads a
secret file, environment variable, mutable definition, or other external input.

This contract complements
[`reconcile-generation-fencing.md`](reconcile-generation-fencing.md). The input
snapshot says exactly what the operation wants to activate. An Action-v3
precondition says exactly which predecessor generation the operation observed.
Neither can substitute for the other, and the desired-input digest is not part
of `ReplicaPrecondition`.

## Snapshot model

The normative model is equivalent to:

```rust
struct ReconcileInputSnapshot {
    schema_version: u32,
    manifest_id: String,
    workload: MachineWorkloadScope,
    environment_generation: u64,
    stack_name: String,
    stack_projection: EffectiveStackInput,
    services: BTreeMap<String, EffectiveServiceInput>,
    secret_blobs: BTreeMap<String, SecretBlobReference>,
    manifest_digest: String,
}

struct EffectiveServiceInput {
    service: ServiceSpec,
    image: EffectiveImageInput,
    networks: BTreeMap<String, NetworkSpec>,
    volumes: BTreeMap<String, VolumeSpec>,
    secrets: BTreeMap<String, EffectiveSecretInput>,
    effective_digest: String,
}

struct EffectiveImageInput {
    requested_reference: String,
    target_platform: String,
    index_digest: Option<String>,
    manifest_digest: String,
    config_digest: String,
}

struct EffectiveSecretInput {
    reference: ServiceSecretRef,
    definition: SecretDef,
    content_sha256: String,
}
```

The types above describe the logical model, not the persistence wire types.
Input `ServiceSpec`, `NetworkSpec`, `VolumeSpec`, and `SecretDef` values are
validated and copied into dedicated `StoredEffective*V1` DTOs. A stored DTO
recursively denies unknown fields and represents every logical map, including
environment, labels, driver options, sysctls, and logging options, as a sorted
sequence of unique key/value records. It never embeds a permissive input type or
`HashMap`.

The v1 wire encoding is compact UTF-8 JSON produced from those fixed-field-order
DTOs, without insignificant whitespace, followed by one newline. Collections
whose input order has no semantics are sorted by their defined identity;
command/entrypoint and other semantic sequences preserve order. Decode validates
schema, uniqueness, sort order, normalized paths/names/numbers, and every
cross-reference, then re-encodes and requires byte-for-byte equality. It rejects
unknown fields, duplicate logical keys, unsafe names, invalid digests,
unresolved references, extra definitions or secret blobs, and noncanonical
ordering. The hash projection uses the same DTOs but a separate length-framed
domain, so digest identity does not depend on incidental JSON object behavior.

`service` is the validated service activation projection with replica count
normalized to one. Replica count controls topology and does not change the
configuration of an individual replica. Every other field that reaches runtime
preparation remains in the projection.

`networks` contains exactly the definitions the service references. An empty
service network list resolves to one canonical implicit `default` definition;
it is not represented as an ambiguous empty set. Top-level networks not
consumed by the service are excluded.

`volumes` contains exactly the top-level definitions named by that service's
named-volume mounts. Bind/workspace mount selectors remain in `service`; their
mutable file contents are data governed by workspace and storage policy, not
container configuration. Unreferenced top-level volumes are excluded.

`secrets` contains every service reference, its exact top-level definition, and
a digest of the immutable source bytes acquired for this operation. The manifest
never contains secret bytes. Each referenced source is materialized once even
when several services consume it. File and environment sources have identical
snapshot semantics. Changing the source locator or the bytes changes the
effective digest; the raw value must never appear in an error, event, audit row,
manifest, or log.

A normal streaming read of a concurrently overwritten file is not a snapshot
and is forbidden. On the 0.4 macOS lane, file capture opens with no-follow and
close-on-exec semantics, verifies a regular file and its identity, and passes
that already-open descriptor—not a subsequently resolved path—to an atomic
copy-on-write file snapshot into the operation-private staging filesystem before
reading and hashing the private snapshot once. Source and staging must reside on
a backend for which this descriptor-based snapshot primitive is proven atomic; an
unsupported filesystem fails before admission. Future host backends must supply
an equivalent immutable/versioned secret provider. A legacy stable-read adapter
may exist outside the certified path only when it locks a cooperating producer,
verifies before/after device, inode, size, change-time, and modification-time,
and compares two complete reads; any disagreement restarts the entire
unadmitted capture, never one field. Environment-sourced capture is serialized
under the process environment snapshot lock and immediately copied into private
staging. Runtime planning never reads either live source.

The stack projection contains validated stack-wide activation settings that do
not belong to one service, including the stack name and applicable storage or
network defaults. These settings have their own digest and reconciliation path;
they are not injected into every service digest merely to force broad
recreation.

The per-service digest is canonical SHA-256 with domain
`vz-effective-service-input-v1` over the full service projection and only its
referenced definitions/content digests. The manifest digest uses a separate
`vz-reconcile-input-manifest-v1` domain and binds the complete scope,
Environment generation, stack projection, sorted service records, secret blob
references, and their digests. Canonical hashing uses framed field names and
lengths; it must not depend on `HashMap` iteration order or ordinary JSON object
ordering.

Mutable image tags are configuration references, not proof of immutable image
content. Before final planning, the scoped image resolver converts every
requested reference for the exact target platform into its OCI index (when
present), manifest, and config content digests. Before any resolver write, the
operation has a durable staging row and expected image inventory. Resolution
may populate only the operation-owned `inputs/images` tree named by that row; v1
has no global pre-admission image cache. Resolver metadata/blob writes advance
durable staging phases and use the same private inventory and cleanup state
machine as secrets. Resolution may not create or activate a runtime generation.
The snapshot binds the requested reference and resolved digests. Runtime
acquisition after action claims and create reverify those digests, and the immutable
generation journal stores the same resolved identity before publication. A tag
whose content changes therefore changes the effective service digest even when
`ServiceSpec.image` is unchanged. Unresolved tags, a resolver that returns only
the input string, or scoped `AllowAll` without verified content identity fail
before planning. The release contract additionally pins every external image
used by the acceptance gate.

## Capture and atomic admission

The current filesystem manifest is useful crash evidence but is not planning
authority. A production operation follows this order:

1. structurally validate the `StackSpec`, resolve its internal references,
   reject duplicates and missing definitions, and allocate one top-level
   apply-operation/manifest identity shared by all convergence rounds;
2. before any external-input or resolver write, create a durable StateStore
   staging record bound to the workload, Environment generation, operation, and
   expected canonical secret/image inventory;
3. resolve each exact-platform image identity and materialize each referenced
   secret source once through the immutable provider rules
   above, read the private snapshot once, write its operation-owned blob with
   create-new semantics, fsync files and directories, and record its byte count
   and SHA-256 without exposing the value;
4. reopen and validate every staged blob, build the canonical effective stack
   and service projections, and compute their digests;
5. invoke `admit_reconcile_round`, the canonical transactional StateStore API
   described below; and
6. only after a nonempty batch's durable Action-v3 claims are acquired may the executor
   create directories, volumes, sandboxes, network allocations, journal
   successors, guest state, or runtime state.

The StateStore staging record makes every durable filesystem phase discoverable.
A crash before snapshot finalization leaves a tracked, effect-free staging
record. Recovery validates its expected inventory: a complete staged inventory
may be finalized only from those same bytes, while an incomplete or invalid
inventory is terminal-failed and scrubbed without rereading live sources. A
crash after finalization resumes only from the immutable snapshot. There is no
filesystem directory whose mere existence authorizes reconstruction from
current external inputs, and there is no active reconcile session without a
finalized snapshot.

`admit_reconcile_round(manifest_id, manifest_digest, round)` accepts either the
complete `staging` snapshot on the first round or that exact `finalized` snapshot
on a later round. In one `BEGIN IMMEDIATE` transaction it revalidates topology
authority and all persisted input projections, finalizes staging when required,
derives the plan from the snapshot plus one consistent predecessor-state
snapshot, and returns exactly one typed outcome:

```rust
enum ReconcileRoundAdmission {
    BatchCreated { session: ReconcileSessionV4, actions: Vec<ActionV3> },
    DeferredOnly { snapshot_id: String, waiting_on: Vec<DeferredService> },
    Converged { snapshot_receipt: RedactedSnapshotReceipt },
}
```

`BatchCreated` is legal only for a nonempty action list. The same transaction
attaches every Action-v3 precondition and persists the session, progress,
actions, and plan hashes. `.9` claim-time admission subsequently revalidates
those preconditions and acquires the durable batch claims before effects.
`DeferredOnly` finalizes and retains a first-round snapshot, or reuses the exact
already-finalized snapshot, without a session. `Converged` creates no session,
terminalizes the top-level apply operation, retains only its redacted receipt,
and advances private inputs toward cleanup. A later round after
`DeferredOnly`, or after a completed nonempty batch with remaining deferred
work, calls the same API against the finalized snapshot; it never restages or
rereads inputs. A terminal `Converged` snapshot cannot admit another round.

Snapshot finalization and any nonempty reconcile-session creation are therefore
one database transaction, not today's separate precondition-capture and
batch-create calls.
Action-v3/store-v6 and claim-authority store schema v7 land first and retain
their published semantics. This input work is store schema v8: it adds immutable snapshot/staging records,
`manifest_id`, `manifest_digest`, and a `plan_hash` to the session/progress/audit
projections. `StoredAction` remains schema 3 because its predecessor identity is
unchanged. A new session/batch envelope schema 4 and domain-separated plan hash
bind the v3 action hash plus the complete manifest identity. The v7-to-v8
migration refuses active schema-v3 sessions because no trustworthy input
snapshot can be reconstructed; terminal sessions are archived before creating
v8 tables. No implementation may retrofit these fields while continuing to
call the result v7 or silently reinterpret Action v3.

## Planning and execution

The planner accepts effective service records, not a mutable `StackSpec`, when
deciding whether an exact replica is current. A running replica converges only
when its journal/observed `applied_config_digest` equals that service's
`effective_digest` and its generation-qualified lifecycle evidence is healthy.
Secret rotation with an otherwise identical service therefore plans one
recreate for every intended exact replica.

Execution resolves the service input by exact target through the persisted
manifest. It verifies manifest, service, definition, and blob digests before any
runtime mutation. Runtime preparation consumes the persisted service projection
and staged secret bytes; it never consults the caller's current spec or reloads
secret sources. The create selector and immutable journal intent carry the
effective service digest, and successful publication copies that same digest to
the observed projection.

Two services may share one staged secret blob, but each effective digest binds
its own reference metadata. Changing an unrelated top-level definition causes
no service action. Changing a referenced network, volume, secret definition, or
secret content affects exactly its consuming services. Definition order alone
does not change semantic digests because capture first produces canonical
logical maps; reordering the already-persisted canonical manifest is tampering
and fails closed.

Non-scoped/legacy planning may retain a test-only compatibility path while it is
being removed from the 0.4 public product. It cannot certify scoped production
behavior or synthesize an effective digest from `ServiceSpec` under a v1 label.

## Recovery, retention, and cleanup

On reopen, a finalized snapshot is immutable. Missing files, unexpected files,
wrong permissions or file type, byte-count mismatch, digest mismatch, malformed
records, a changed owner/generation, or disagreement among snapshot, session,
audit, action, and journal identities is a state conflict before mutation.

Snapshots and secret blobs remain referenced while the top-level apply
operation, a reconcile session, batch audit claim, create intent, or published
generation can require replay or provenance. All deferred rounds under one
apply operation reuse the same finalized snapshot. A round with no actions but
remaining deferred work creates no empty batch and retains the snapshot under
the active apply operation. A terminal no-action result records the redacted
snapshot/digests, marks its secret bytes retireable, and creates no reconcile
session. Failure, cancellation, or deadline terminalizes the apply operation
before retiring its inputs.

Cleanup is a durable state machine. In one transaction it proves that no live
reference remains and marks the exact manifest `delete_pending` with a fresh
cleanup nonce and complete expected file inventory. It then atomically renames
the operation directory on the same filesystem to its nonce-qualified private
trash path and fsyncs both parents. Recovery can distinguish not-yet-renamed
from renamed state from the row and the two exact paths. It securely unlinks
only the inventoried private files/directories, fsyncs the trash parent, verifies
both paths absent, and finally records a redacted tombstone in a transaction.
A crash before rename retries the rename; a crash after rename retries deletion;
a crash after deletion but before the tombstone verifies absence and completes.
It never commits `deleted` before unlink and never unlinks before the durable
`delete_pending` record. A failed cleanup is retryable and does not make the
operation reconstructible from live sources.

V1 does not deduplicate secret blobs across operations or Environments. A later
shared content-addressed store requires explicit reference accounting and
Environment-scoped encryption; discovering equal bytes never grants
cross-Environment access.

Terminal archival may retain redacted definitions and content digests, but not
secret bytes. The release evidence secret-canary scan covers the state store,
manifests, logs, errors, and retained artifacts.

## Required adversarial gates

Deterministic tests use barriers and separate StateStore connections; timing
sleeps are not race evidence. They prove at least:

- rotating only file-secret bytes and only environment-secret bytes plans and
  executes recreation for every intended exact replica;
- `api` replica 2 and `api-2` replica 1 never alias in snapshot lookup, digest,
  plan, audit, journal, or outcome;
- changing a referenced network, volume, or secret definition affects exactly
  consuming services, while changing an unrelated top-level definition has no
  effect;
- resolving the same tag to new OCI content recreates exactly its consumers;
  wrong-platform, string-only, or changed pull results fail before publication;
- source mutation between capture, planning, batch claim, preparation, and
  publication cannot mix old and new bytes or digests;
- crash/reopen at every staging/finalization/session boundary either finalizes
  the complete tracked inventory from the exact staged bytes, terminal-fails
  and scrubs incomplete staging, or resumes the exact finalized snapshot;
- missing, extra, duplicate, reordered, malformed, symlinked, permission-changed,
  truncated, or content-tampered manifest inputs fail before resource/runtime
  mutation;
- session/action/audit/journal manifest IDs or digests cannot be swapped,
  replayed under a different scope/generation, or downgraded to the old
  `ServiceSpec`-only digest;
- concurrent controllers cannot finalize different snapshots for the same
  operation or execute an action against a snapshot it did not claim;
- cleanup never removes a blob still referenced by a resumable operation or
  another exact generation; every crash point in the
  delete-pending/rename/unlink/tombstone state machine resumes without loss or
  untracked bytes and removes terminal secrets without evidence leakage;
- no-action, mixed actionable/deferred, deferred-only, deadline, cancellation,
  and restart paths retain or retire exactly the top-level apply snapshot
  required by their state and never create an empty reconcile batch; and
- the physical local-Mac Linux-Machine lane repeats secret rotation and
  crash/reopen through the installed public CLI/API and records exact replica,
  manifest, generation, runtime, and no-leak evidence.

Passing unit tests alone does not close `vz-mzs.2.5.5.8`; the applicable
release-built local-Mac gate and aggregate 0.4 evidence remain mandatory.
