# Installed local-Mac Compose and Buildx slices (DEV)

`scripts/run-linux-docker-e2e.sh` is the host entry point. `--suite compose`,
`--suite build`, and `--suite artifacts` select distinct DEV slices. `--suite all` rejects before client execution, state creation,
or VM provisioning; it never aliases the full 63-scenario contract to a subset.

Pass absolute paths for every artifact/client and a fresh evidence directory:

```sh
scripts/run-linux-docker-e2e.sh --suite compose \
  --release-dir /absolute/signed-release/bin --release-version 0.4.0-dev \
  --developer-bundle /absolute/developer-bundle \
  --hardened-bundle /absolute/hardened-bundle \
  --docker /absolute/docker --compose-plugin /absolute/docker-compose \
  --buildx-plugin /absolute/docker-buildx \
  --evidence-dir /absolute/fresh-evidence --run-id compose-unique-candidate
```

The optional `--fixture` and `--image-input` default to the repository fixture
and `python-image-input.json`. The latter contains exact raw registry index,
ARM64 manifest and config metadata with independently checked SHA-256 links.
It is metadata provenance, not prior image execution evidence. The runner must
pull the immutable digest into each selected vz Machine, inspect its config and
platform, and execute its Python/shell before building the Compose image.
For the selected containerd-backed Engine, the expected image ID is the exact
ARM64 manifest digest, not its config digest. Inspection must match the manifest
descriptor, repository digest, platform, config fields and ordered diff IDs.
The separately recorded config digest is verified registry provenance, not a
claim that raw config bytes were independently fetched from the live Engine.
Before fixture mutation, public Exec verifies the pinned public CA bundle hash
at `/etc/vz/ca-certificates.crt` inside every exact Machine, without replacing
the image's distro trust store. Before each pull, host Engine info binds the Engine
ID to secure Docker Hub configuration with no mirrors or insecure ranges beyond
Moby's loopback defaults. This does not claim a complete effective-trust audit;
the immutable HTTPS pull must still succeed. Host keychains are not imported.

The runner stages byte-identical signed CLI/daemon and host plugins into a fresh
private prefix. Normal public Up launches the installed daemon and four
Developer-profile Linux Machines: two named Environments, two Machines each,
one project/worktree. It does not modify the daily installation, HOME, or daily
Docker default. All Docker operations name the exact private config and Machine
context; there is no global daemon fallback. The public Up activation binds the
real runtime identity and incarnation to public status, the installed catalog,
startup receipt, post-startup inventory, context endpoint and Engine identity.
These checks precede fixture mutation.

Eight real Compose recipes run on both primary Machines and one neighboring
Machine. Three separate driver outputs retain all stdout/stderr, exit codes,
timestamps, pre-dispatch intents, semantic acknowledgements for expected
negative mutations, input pins and checksums. Independent replay requires the
actual create/start/health events, exit37 streams, complete network denial
matrix with paired destination controls, a host-written persistence sentinel,
scale identities, blocked dependencies, failure propagation and owned cleanup.
Missing history or observations fail; no test retries are performed.

For Buildx, select `--suite build` and additionally pass
`--buildkit-archive /absolute/vz-buildkit-v0.19.0-linux-arm64.tar`.
The archive must match the checked-in runtime-free BuildKit pin, not a
caller-selected checksum. Each selected Machine gets a separately owned
`docker-container` builder, image and cache volume with the installed pinned
youki runtime. Builder registration does not change the default builder or
Docker context. The five recipes exercise multi-stage local output, repeated
vertex cache reuse, build arguments, cold/warm cache mounts, and required-secret
positive/negative behavior. Independent replay checks raw command evidence and
retained export bytes before parent cleanup is admitted.

The imported builder reproduces the pinned upstream image's
`BUILDKIT_SETUP_CGROUPV2_ROOT=1` setting and explicitly selects a private cgroup
namespace. Image/container inspection rejects missing, duplicate or altered
environment values and namespace/runtime drift. Each recipe also binds the
same live builder PID and complete start timestamp with zero restarts.
Before and after the workload slice, a recorded public Machine Exec reads the
builder's projected cgroup filesystem externally. It requires an empty domain
root, enabled controllers and the inspected init process in its domain leaf;
it does not migrate processes or enter the builder's namespace. This proves
initialized root state, not a snapshot of each ephemeral workload cgroup.
Ordinary Docker exec and all recipe assertions remain required. A failed
bootstrap retains a separate external diagnostic without retrying or repairing
the failed builder.

The builder may fetch the exact pinned base image using HTTPS. Fixture RUN
steps disable networking; this does not make the complete build offline.
These recipes do not establish cache export/import, cross-Machine cache denial,
parallel builds, SSH forwarding or secret absence from all image/cache blobs.
Source integration and offline tests alone are not physical Buildx evidence.

The separate `--suite artifacts` lane also requires `--buildkit-archive`. It
preserves the five-recipe build contract and uses three distinct owned builder
roles per tested Machine: source, cold-control, and importer. Source exports
alpha OCI plus local cache, beta OCI, and an uncached secret build's OCI plus
local cache. Identical alpha inputs must execute on the fresh no-import control
and hit on the different fresh importer using only the exact source export.
The same run token and fixture bytes are used on all three tested Machines;
owner identity never becomes an argument that manufactures a cold miss.

This bounded exporter policy is pinned to BuildKit 0.19: OCI directory output
(`tar=false`), OCI media types, forced gzip layers, no provenance/SBOM, and fresh
local cache output with `mode=max,image-manifest=true`. It does not claim
timestamp normalization or byte-reproducible image manifests. Streaming checks
verify descriptor hashes/sizes, config diff IDs, complete cache graph references,
exact final scratch payload, changed alpha/beta layers, and secret canaries in
metadata plus all raw/decompressed exported blobs. Intermediate canaries are
excluded from final OCI files, not from the cache where they legitimately belong.
Unsupported formats, unreferenced blobs, unfinished ingest files, links in the
host artifact tree, and changed exports fail closed. Exported-cache inspection
is not a complete worker-cache audit.

Each artifact operation has nine raw commands: four identity/process guards,
one solve, and four post-solve guards. Independent replay binds exact arguments,
Engine clocks, cache-hit/miss execution markers, continuous builder identity,
and the before/after imported cache inventory. Normal cleanup is withheld until
all five operations and role ownership checks pass. This lane's implementation
and offline tests are not a physical pass; consult its retained candidate result.

Artifact candidates 1 and 2 remain failed and preserved. Candidate 1 exposed an
implicit fixture file-mode assumption; public outputs now explicitly use `0644`.
Candidate 2 completed the source exports and fresh cold control, then rejected
the importer's nested lazy-cache progress as graph drift. That importer emitted
no worker-runtime invocations. This is diagnostic evidence, not an accepted cache
hit. The replay now separately validates complete, nested lazy-materialization
records and the exact imported-manifest status; the ordinary graph checks remain
strict. A fresh candidate is still required.
Their separate disposition records prove positive Stops without removing failed
Docker objects or disks. Historical fixture bytes are retained separately under
`.artifacts/docker-artifact-integration-2g6xLx/original-fixture`.

Four additional containers are started once before workload execution. A
separate recorder continuously checks their Engine identity, unchanged start
time/ID, zero restarts and unique host-written marker. These are contemporaneous
container liveness observations, **not** public-network service conformance.

Cleanup removes only positively inspected owned workload objects and images,
then obtains public Stop receipts and graceful daemon shutdown. Unknown command
effects withhold cleanup; they are retained durably, including normal nonzero
mutation failures. A monitor is positively reaped before removals begin. Outer
cleanup also requires each selected driver's successful cleanup and
independent raw-evidence replay; certain command exits alone cannot certify
that volumes or networks disappeared. Missing or failed replay retains resources.
For the Buildx slice, exact owned builder/container/cache-volume removal precedes
the shared image cleanup. The owned installation, stopped Machine disks/contexts
and embedded Engine BuildKit cache remain
for inspection. This is not Delete acceptance or a complete leak audit.

No result certifies Docker parity or emits release-scenario PASS. Full fixture
freezing, all63 scenarios, full OCI/cache/runtime audits, host binds/forwarding,
recovery, native macOS, the five-verb Delete path, and the canonical three-phase
aggregate with measured hardware sleep remain separate required work.

## Registry slice (DEV, not release)

`scripts/run-linux-docker-registry-e2e.sh` is the only host entry for
`--suite registry`; it fixes the suite (passing `--suite` is an error) and runs
the dispatcher under `uv run --no-project --python /usr/bin/python3
--with-requirements scripts/helpers/registry-requirements.txt`. The hash-pinned
`cryptography`/`bcrypt` dependencies exist only for that process; nothing is
installed into system Python or the user site. The registry suite additionally
requires `--registry-archive` (the deterministic load TAR, e.g.
`.artifacts/registry-3.1.1-load-candidate-1.tar`) and `--registry-layout` (an
admitted selected-only OCI layout). It rejects builder, parallel, SSH, container
fixture and tmux options, and `--suite all` is still rejected before any client
execution. See `docs/docker-registry-acceptance.md` for the acceptance contract.

Before startup preflight, and before any client execution, state creation or
VM provisioning, the runner admits the registry inputs read-only against
`config/docker-registry-artifact-v3.1.1.json`: exact layout inventory and every
decoded layer diff ID; byte-for-byte archive replay against that layout; the
unexecuted `bin/registry` binary (SHA-256 `669f0d98...572834`, Go 1.25.9,
Distribution v3.1.1) read from the pinned layer stream; and the isolated Python
dependency versions against the lock. Every registry helper
(`linux_docker_registry_*.py`, `linux_docker_private_stdin.py`, the pin, the
lock, the Machine adapter and the wrapper) plus the archive digest are frozen in
`inputs.json` and rechecked at the end of the run.

Normal public Up still provisions two Environments with two Developer Linux
Machines each. The registry recipe runs on three selected Machines (both
primary, first neighbor); the second neighbor Machine is only a same-authority
credential sentinel. Every Machine uses the same private nonloopback authority
`172.30.241.2:5443` on its own internal Docker network with distinct in-memory
secrets, so cross-Machine credential controls observe all four Machine-private
Docker config directories: a baseline before the first Machine, after each
valid login, after each logout, and a final check. Per Machine the sequence is
prepare (load, private guest fixture, network/volume/server), authenticate
(wrong CA, wrong password, unauthenticated push, then one route-attributed
valid login), roundtrip (push, remove local references, pull by digest,
export and compare bytes), independent raw-command replay plus secret-canary
scan, and only then the exact owned cleanup and logout controls. No exception
path calls cleanup; an incomplete or uncertain Session stays registered and
withholds the harness's normal sentinel removal and public Stop, recording the
retained state in `cleanup_errors`. Private material travels only through
bounded private stdin and is never documented; evidence documents are rejected
if they contain a fixture canary.

This slice is `DEV_INSTALLED_LINUX_REGISTRY_LOGIN_PUSH_PULL_NOT_RELEASE_CERTIFICATION`.
It does not close `docker.image.registry_login`, `docker.image.pull` or
`docker.image.push`, does not prove a directly captured `/auth` request, and
does not cover Stop/recovery credential persistence or migration. No physical
registry candidate has been run through this wiring yet.

## Buildx candidate 1: failed, retained

`.artifacts/linux-docker-build-candidate-1` exercised the signed installed
`topology-cli-installed-gLq7X5` artifacts on the local Mac. All four Developer
Machines became ready, their sentinels started, and the first Machine pulled
and executed the pinned Python input and built the preparatory image through
its embedded builder. The separate BuildKit builder container then failed OCI
create through pinned youki (command 080). None of the five-recipe Buildx slices
ran. This is a failed candidate, not a passing compatibility demonstration.

All 1,497 raw artifact hashes and 80 command receipts were independently
verified. Its evidence manifest SHA256 is
`65c2ea986b44e7fad09719d7b64b15f59a9c46cedda8853c9711ee50d5530b62`.
The failed mutation remained uncertain and withheld Docker-object cleanup.

A separate `.artifacts/linux-docker-build-candidate-1-disposition` captured
bounded guest daemon logs through public Exec, positively stopped all four
Machines, and gracefully closed the exact original daemon. Docker defaults
were unchanged. The original failed evidence, Docker objects and stopped disks
at `/private/tmp/vzdev-c4q2h8zh` remain preserved; no retry, disk repair, resource
deletion or candidate promotion was performed. The generic OCI-create error
still needs a concrete runtime diagnosis before the next candidate.

## Buildx candidate 2: actionable failure, retained

`.artifacts/linux-docker-build-candidate-2` uses the logging-corrected youki
runtime after the complete selected Mac backend gate passed. Both public Ups
and all four startup probes passed. The first preparatory image build also
passed, but command 080 again failed before the five-recipe slice. Containerd
now preserves the actual error: `/sbin/docker-init` does not have correct
permissions. All 1,509 raw files were independently verified; manifest SHA256:
`6612082e3bd313e68a37f87e5e34d53a37402131f6add9f11021792974ca998e`.

The separate public-Exec/Stop disposition confirms the exact pinned guest
`docker-init` is a root-owned regular executable with mode `0700`. Pinned
youki's preflight incorrectly checks only the other-execute bit (`0o001`),
rejecting this owner-executable file before kernel execution. The correction
is tracked as `vz-pa9`; private artifact modes, `--init`, privileged builder
settings, and youki-only execution must remain unchanged.

All four Machines were positively stopped and the original daemon gracefully
closed. Failed Docker objects and disks at `/private/tmp/vzdev-1vxano0n` remain
preserved. Disposition manifest SHA256:
`981c504b72105698bbf86c30aa177b443d01ab7b22df4e2f19ff2cd4104c3401`.
This proves actionable runtime diagnostics, not a passing Buildx slice.
