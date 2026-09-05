# Developer startup readiness (DEV)

Normal Linux-on-macOS `vz up` measures operational readiness through the exact
Machine's retained VM and private Docker endpoint. It does not run or require
the complete Docker compatibility release suite on each startup. Release-wide
compatibility and the 0.4 aggregate remain separate acceptance gates.

The installed catalog supplies verified profile-qualified artifact pins (see
[installed catalog](installed-machine-catalog.md)). The Developer bundle now
contains a deterministic offline BusyBox rootfs with versioned public marker;
its archive SHA and build provenance are bound by the bundle's version metadata.
Legacy bundles without that input cannot silently substitute an image or daemon.

Startup measures POSIX execution and inventories executable paths and pinned
daemon mounts in the original VM. It verifies the actual youki binary against
the Machine's artifact digest before and after Docker operations. Moby's inert
built-in runtime-name metadata does not count as an installed runtime: the
default and every inspected probe container must use youki. This startup check
does not replace the release gate's recursive cached-artifact/execution audit.

The host adapter discovers the installed Docker executable, or uses the exact
absolute `VZ_DOCKER_CLIENT` override. It uses `VZ_DOCKER_CONFIG`, then
`DOCKER_CONFIG`, then the normal user Docker config directory. Compose and buildx
must be installed for that client. The executable is canonicalized and hashed;
each bounded command verifies those bytes again. On macOS, the native
administrator-managed `/Applications` directory is an explicit executable trust
boundary; Machine-store and Docker-config ownership checks are not relaxed.

One stable context is create-only, backed by a private durable claim containing
the exact Project, Environment, Machine, endpoint, config directory and nonce.
An existing foreign, edited or malformed context is not adopted or overwritten.
No operation selects a global/default Engine or changes the user's default
context. Status returns an opaque context descriptor with the persisted
incarnation and Engine identity; its availability label is explicitly persisted
state, not a live health measurement.

Bounded offline operations import the verified rootfs, create/start/exec a
container, bring up/exec/down a Compose service, and build/load/run a scratch
image through that context's embedded `docker` buildx driver. The build includes
an uncached guest `RUN`, whose output is checked in the loaded image. No registry base,
frontend download, alternate runtime, `docker-container` builder bootstrap or
fallback daemon is used. Exact disposable probe containers, Compose objects and
images must be removed before a successful immutable receipt is emitted. Normal
Machine-owned BuildKit cache is explicitly retained; no global prune is allowed.

The private per-Machine journal admits each command before dispatch and retains
bounded raw stdout/stderr and checksums. An uncertain mutation is not retried or
declared absent. Such failures retain original Machine ownership for `vz stop`
and require exact journal-aware recovery before another probe can run. That
mutating-failure recovery, ownership-safe Delete/context removal, host bind-path
translation and the full compatibility matrix remain unfinished.

A repaired host-client prerequisite may be checked by a subsequent Up only when
the retained, exact-owner journal positively proves that every admitted command
was a recognized read-only query and no resource mutation was admitted. The
failed attempt is archived before supersession. This does not replay a failed
mutating command or provide mutating-failure recovery.

Stop retains the logical context descriptor while removing its live socket.
Subsequent Up must preserve the context's owner, name, endpoint and config
directory while binding new activation evidence to the new incarnation. Native
macOS and Hardened Machines do not acquire implicit Docker capabilities.

This Stop/Up behavior assumes the original daemon remains alive. Supported
daemon-crash recovery is still missing: a stale control socket blocks autostart,
and a new daemon cannot adopt previously active Machines without authoritative
backend reconciliation. See the [catalog recovery limitation](installed-machine-catalog.md).

The separate [installed startup harness](../scripts/helpers/installed_developer_startup.md)
tests this path using signed staged product binaries and real local vz-managed
Machines; passing it is DEV operational evidence, not 0.4 release certification.

## Local-Mac evidence: 2026-09-05

`.artifacts/installed-developer-startup-candidate-2/` passed the installed startup
harness with four Developer Machines across two named Environments in one
worktree, plus a sequential Hardened Machine. Six distinct activation attempts
(four initial, two after Stop/Up) each completed 45 startup commands, including
uncached Buildx execution. All six immutable receipts have matching, separately
retained before/after runtime inventories. Three additional independent host
Docker/Compose/buildx workloads passed through the managed contexts.

Environment/Machine/context identities and Engine IDs persisted across Stop/Up;
the neighboring Environment remained usable. Hardened acquired no Docker
capabilities or descriptor. Final public Stop receipts, socket removal, observed
graceful daemon termination and unchanged daily/isolated Docker defaults passed.
All 1,124 evidence-file checksums were independently verified. The exact signed
CLI/daemon are retained in
`.artifacts/developer-startup-release-SlDuQ1/signed-inventory-root/`.

The preceding `installed-developer-startup-candidate-1` failed before Docker
workloads because its new inventory expected the outer-init runtime mount inside
the overlay chroot. That candidate and successful Stop/daemon cleanup remain
retained; the corrected candidate verifies the actual guest runtime copies.
These are distinct development candidates, not retries within a release gate.

Remaining findings are tracked: `vz-7ez.1` covers mutating-probe recovery;
`vz-7ez.2` covers macOS full-close shutdown warnings without masking truncation.
`vz-ehz` covers exact daemon/control-socket recovery after a crash.
The passing workload results do not certify every stream-close condition,
named-volume/workspace semantics, native macOS, the 63-scenario Docker contract,
or the mixed-target 0.4 aggregate. Nothing was installed over the daily binaries.

## Backend regression checkpoint

The required `scripts/run-sandbox-vm-e2e.sh --suite all --profile release` run
passed at `.artifacts/sandbox-vm-e2e/20260905T183551Z/summary.txt`, with raw logs
and pinned youki/BuildKit runtime inventory retained beside it. Selected checks:
runtime 19, two generation-recovery checks, daemon teardown 1, Machine registry 1,
stack 24 and BuildKit 3. The harness's defined test selection remains unchanged;
this is backend regression evidence, not the missing 63-scenario Docker gate.

Three preceding full-run failures remain retained: `20260905T172026Z` exposed
the crash fixture's unsafe stale-socket assumption; `20260905T175255Z` exposed
host tests incorrectly included in its exact physical-test inventory;
`20260905T181407Z` exposed a four-file registry fixture copy that omitted the
newly declared probe. The corrected crash fixture removes only its captured
socket after positively reaping its exact child. Its host regressions run in a
separate target, and all 18 physical cleanup receipts passed strict validation.
This fixture authority does not implement user-facing daemon-crash recovery.

Registry fixture copies now pass the production verifier before and after copy,
and evidence binds the exact optional probe digest/file identity to retained
version bytes. The focused physical run `20260905T183319Z` and the subsequent
full run passed. The installer legacy alias also carries only the declared,
verified probe; replacing or removing an old probe requires its previous
declaration and matching bytes. Eleven installer regressions passed.
