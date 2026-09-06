# Explicit host-client fixture driver (DEV)

`docker_host_driver.py` executes the checked-in Docker fixtures through the
unmodified installed Mac Docker CLI and its Compose/buildx plugins. It is **not**
`run-linux-docker-e2e.sh`, a compatibility report, or a release-gate substitute.
Standalone driver output does not certify the installed physical boundary; see
[the installed harness](linux_docker_e2e.md) for its qualified DEV slices. The 63
required catalog scenarios and full aggregate remain open.

The recorder also supports explicit bounded stdin/PTY action plans. Their
[capture and replay contract](../../docs/docker-container-io-evidence.md) remains
test infrastructure: the installed container-lifecycle lane is not yet complete.

The executable subset currently covers:

- Compose create, healthy up with Engine-event dependency ordering, exact
  stdout/stderr/exit 37 exec, declared network membership and denial by name and
  actual IP, volume persistence across Compose stop/up, three-replica scale-up
  and exact scale-down identities, unhealthy-dependency blocking, failure exit
  propagation and retained partial resources;
- BuildKit local multi-stage exports, build argument variation, explicit raw
  vertex cache-hit/miss observations, cold/warm cache-mount contents, and positive
  and uncached missing-secret builds;
- exact project-labelled and fixture-owner-labelled cleanup; no global prune,
  image removal, builder removal, context switching, guest substitute client,
  Docker Desktop, inherited Docker/Compose/buildx variables or user SSH agent.

Successful recipes emit `fixture_assertions_passed`, never release-scenario
`PASS`. Related catalog IDs are references, not claims that every assertion of
that scenario passed. In particular, local payload comparisons do not certify
OCI image/layer digests, cache import/isolation, or complete secret-leak scans.
The result fixes `compatibility_certified: false` and `release_scenarios_passed:
[]`; an aggregate validator must never silently promote it.

## Provisioned inputs

The owning topology harness must supply a fresh run ID and independently verify
all following inputs. The input file is not an authentication mechanism or proof
that a socket belongs to a Machine. The driver also verifies current context,
Unix endpoint, actual Engine ID, Linux/arm64 target, youki runtime configuration,
pinned images and exact builder container before sending fixture workloads.

Required JSON keys (unknown keys fail):

| Key | Required value |
| --- | --- |
| `schema_version` | `1` |
| `run_id` | Unique lowercase/digit/hyphen token, 8–40 characters |
| `release_sha256`, `fixture_sha256` | Actual verified lowercase 64-hex digests |
| `scope` | Exact nonempty `project_id`, `environment_id`, `machine_id`, `machine_incarnation`, `runtime_identity`, `docker_context`, `docker_endpoint`, `engine_id` |
| `docker_config` | Canonical absolute disposable directory, mode `0700`, not the user's `.docker` |
| `clients` | `docker`, `compose`, `buildx`, each `{ "path": <canonical installed binary path>, "sha256": <actual digest> }` |
| `images` | `base` and `compose`, each `{ "reference": <immutable reference>, "id": <verified sha256 image config ID>, "platform": "linux/arm64" }` |
| `builder` | Required for `build`/`all`; optional (not null) for `compose`. Exact `name`, `node`, `container_id` and `image_id` of a fresh, already-running owned docker-container buildx builder |
| `runtime_evidence` | Optional exact `receipt_path`, `receipt_sha256`, `inventory_path`, `inventory_sha256`, `youki_sha256`; required when Engine advertises inert stock runc metadata |

`images.base.reference` must be a repository `@sha256:` digest and provide Python
3 and `/bin/sh`. `images.compose` is built from the fixture `compose/Dockerfile`;
its reference can be its immutable local image ID. Both images must already be
present on the selected Machine with matching platform and configuration ID.
There is no mutable image or missing-image fallback.

The builder must be isolated to this candidate and Machine and must use a
verified **youki-only** image. The stock BuildKit image is not automatically an
acceptable input. The driver does not bootstrap or create a builder, attach one
to another endpoint, or remove the caller's builder/cache. The surrounding gate
owns its provisioning, recursive runtime inventory and final destruction.
Compose-only execution neither requires nor inspects a builder. The Python API
admits inputs with `Inputs(raw, suite="compose")`; the default remains `all` and
requires a builder. Switching suite after admission is rejected before commands.

Runtime evidence paths must be canonical, bounded regular files with their exact
SHA-256 digests. The completed immutable operational-probe receipt and adjacent
post-probe inventory must bind the input Project, Environment, Machine,
`machine_incarnation` (the exact incarnation ID), Engine, managed context,
Docker executable and pinned youki digest. They are re-read and rehashed on every
guard. Stock `runc`/`io.containerd.runc.v2` entries are accepted only as exact
`{"path":"runc"}` metadata alongside this verified executable-absence evidence;
the selected default must remain youki at `/mnt/linux-bin/youki`. This is the
normal startup executable-path/pinned-mount inventory, **not** a recursive cache
audit or full release runtime attestation. The parent must independently bind the
receipt to the actual current status and signed release; supplying JSON alone
does not authenticate ownership or runtime identity.

The private client directory must already contain the managed Machine context.
Its `config.json` allows only `currentContext` and `cliPluginsExtraDirs`; the
latter is the sorted unique parent directories of the pinned Compose/buildx
binary paths. Alternatively, the installed harness may supply mode-0700
`config/cli-plugins` containing exactly the pinned regular executable bytes under
`docker-compose` and `docker-buildx`; this layout forbids `cliPluginsExtraDirs`.
Mixed layouts, unknown discovery plugins, redirects and changed bytes fail.
Credential helpers, auths, proxies, shadow plugins and unknown
client settings are rejected. The driver never changes `currentContext`, and
every invocation includes the same explicit `--config` and `--context`.
The real host `HOME` is preserved (or remains unset); only `TMPDIR` selects a fresh
owned directory. Docker/Compose/buildx and SSH-agent environment variables are not
inherited. Canonical multicall Docker bytes are executed with logical argv0
`docker`; plugin copies retain their exact required discovery basenames.

Calculate the fixture digest with the driver's `tree_digest(Path(...))` (the
contract's sorted `[relative_path, mode, size, SHA256]` JSON inventory algorithm).
This is an input observation, **not** freezing the incomplete fixture bundle.
Do not include Python bytecode caches in the tree.

## Invocation and evidence

After the owning harness has created and authenticated the exact live Machine:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/helpers/docker_host_driver.py \
  --inputs /absolute/owned/verified-inputs.json \
  --fixture /absolute/checkout/tests/fixtures/vz-0.4/docker \
  --output /absolute/owned/fresh-evidence-directory \
  --suite all
```

`--suite compose` and `--suite build` select subsets, not release-lane waivers.
The result records the selected suite. A successful subset requires exactly its
ordered inventory: five BuildKit recipes, eight Compose recipes, or all thirteen.
Every successful recipe must have a nonempty, non-overlapping command range
within `1..command_count`; unexecuted, missing, extra or duplicate recipes cannot
pass. These inventories do not replace the full 63-scenario release contract.

Every command durably records an inflight intent before dispatch, then its pinned
executable, logical `argv[0] = docker`, arguments, fixture environment additions,
stream observations, exit, timeout and wall/monotonic timing. stdout and stderr
are drained concurrently with a **4 MiB retained bound per stream**. At most one
additional byte is read to detect overflow. Excess output is a failure: the
owned CLI/plugin process group is killed and given a bounded five-second reap;
a failed reap remains uncertain. Bounded prefixes and their
hashes are retained, and `capture_complete` / `raw_streams_retained` are false.
Full raw-stream hashes are null when capture is incomplete; retained-prefix
hashes never masquerade as hashes of discarded or unobserved output.

Timeout, interruption, excess output or unknown dispatch effects remain failures,
never expected negative outcomes. Killing/reaping the host client does not prove
its daemon-side operation stopped mutating. Automatic destructive cleanup is
withheld whenever any command has uncertain effects, including uncertainty that
arises during an earlier project's cleanup. The result identifies owned projects
requiring topology-level reconciliation. There are no test-case retries.
Normal nonzero exits from potentially mutating commands also remain uncertain.
Only an exact expected-negative semantic proof can write a separate durable
`command-NNNNN.acknowledgement.json` binding the unchanged terminal receipt hash.
The terminal receipt itself retains its original uncertainty; consumers must
verify the acknowledgement and replay its proof, never treat host exit alone as
daemon-side quiescence.

Compose admission and cleanup inspect exact generated volume/network names even
without labels, reject collisions, and reconcile actual mounts/network IDs to
captured owned resources before `down --volumes`.
After down, exact generated names must also be absent independently of labels,
and every container ID observed immediately before down must return the exact
missing-container response. Empty label queries alone cannot prove cleanup.
Persistence uses an exclusive
host-written marker, not the startup-recreated database sentinel. Every denied
name/IP transport probe is bracketed by exact source/destination healthy-byte
controls; HTTP errors and unclassified failures cannot prove isolation. Blocked
health requires four concrete created identities and positive Engine creation,
database-start and unhealthy history; empty event windows cannot pass. Failure
propagation requires the observed failure exit and no service left running.

The fresh mode-0700 output directory contains numbered command receipts, raw
streams, owner overlay, local exports, `inputs.json`, `result.json` and recursive
`checksums.sha256`. Secret canaries found in raw or base64-encoded BuildKit log
streams are withheld with an explicit failure and original hash. This only
protects command streams; exported image/cache blob scanning remains required.
Treat this private DEV evidence as untrusted until the aggregate's comprehensive
secret and provenance validators pass. Private temporary state is not public evidence.

Validate `result.json` with `docker_host_results.schema.json` **and** the driver's
`validate_result` semantic checks. The latter checks exact suite order and
cross-field command bounds/non-overlap, which JSON Schema cannot establish.
Schema validation alone does not verify raw
receipts or ownership; the future aggregate must independently recompute hashes
and replay assertions against the exact release/topology evidence.

Offline tests (no Docker invocation):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/helpers -p docker_host_driver_test.py
PYTHONDONTWRITEBYTECODE=1 uv run --offline --with jsonschema==4.23.0 \
  python -m unittest discover -s scripts/helpers -p docker_host_schema_test.py
```

## Still required

Physical execution; authenticated runtime/owner provenance; full cache export /
fresh-builder import and cross-Machine tests; OCI metadata/layer comparisons;
all-layer/decompressed-cache secret scans; complete SSH support; Compose live-log
stream tests; unrelated cleanup decoys and surviving-environment health; full
registry/container/storage/network/resource-pressure/recovery coverage and the
single aggregate release run remain open.

SSH needs a pinned Linux/arm64 Python+OpenSSH client/server base, fresh fixture
agent and keypair, exact server host key and authorized key provisioning, and a
declared builder-to-server network path. No user agent, external SSH host, host
network entitlement or implicit sidecar is an acceptable substitute. This driver
does not yet execute the SSH fixture.

Command behavior references: [Buildx raw JSON progress](https://docs.docker.com/reference/cli/docker/buildx/build/#set-type-of-progress-output---progress)
and [Buildx inspect](https://docs.docker.com/reference/cli/docker/buildx/inspect/).
`buildx inspect` has no `--format` option; exact single-node identity fields are
parsed fail-closed from its retained text output.
