# Docker fixture subset for vz 0.4

These are executable workload inputs for a future host-client harness. They have
not run in Docker. They do not freeze the complete fixture bundle, certify any
scenario, or cover the full 63-scenario requirement catalog. The authoritative
requirement input remains [the draft contract](../../../../config/docker-compatibility-v0.4.json).
`fixture.json` names exact payloads, templates, negative controls, and outstanding
work. No test result belongs in that manifest.

Run the offline checks from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 tests/fixtures/vz-0.4/docker/validate.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests/fixtures/vz-0.4/docker -p test_fixture.py
```

The validator emits actual SHA-256 values for the four fixed output payloads and
the secret input. These are file-content digests, not OCI image digests. Resolve
owner templates from the retained owner mapping, encode UTF-8 without translation,
and hash those exact bytes. Image/OCI archive/cache digests remain unresolved
until a reproducible build and export are performed.

## Inputs and invocation boundary

The future harness must reject mutable references **before invoking Docker**.
Use `validate_inputs` (or the optional `--base --image --owner` validator flags)
for syntax, then independently verify the selected platform and actual content
digest against the release input inventory. A syntactically valid hash is not
proof the artifact exists. All Dockerfiles have a required base argument with no
default; their in-build guards are defense in depth and cannot prevent an
unvalidated reference being resolved before the first RUN.

`FIXTURE_BASE` supplies Python 3 and a POSIX shell. For SSH build Dockerfiles,
pass the verified OpenSSH-capable base as `FIXTURE_BASE`.
`FIXTURE_SSH_BASE` supplies Python, OpenSSH client/server, an unlocked root
account usable for public-key login, and /bin/cat. No package manager or mutable
download runs inside a build. Each base must target linux/arm64. No external
`# syntax` image is fetched: these fixtures use the frontend bundled in the
pinned BuildKit/Engine, whose identity the harness must retain.

Build with context `build/`, `compose/`, or `ssh/` respectively, never this
parent directory. Credentials are outside those contexts. Store secrets, caches,
local output, builder state, and receipts in distinct owned temporary paths.
Use only the installed Mac Docker/Compose/buildx clients, an explicit selected
Machine context and exact run-owned resource identities. There is no lane
driver here; commands below describe workload parameters, not permission to
connect to a default daemon.

## BuildKit workloads

`build/Dockerfile` has a producer and a scratch output stage. Supply
`FIXTURE_VARIANT=alpha` (then beta for argument variation) and one
`FIXTURE_RUN` token shared across this candidate's comparisons. The final stage
must contain only `payload.txt`; the intermediate marker and tool source must
be absent. Use the same final stage for local output and OCI output, retaining
actual content, configuration and layer digests. Byte-identical payloads alone
do not establish reproducible image metadata.

For layer-cache reuse, the harness records BuildKit vertex results and requires
the payload RUN to execute once and be cached on an identical second build.
Export with `--cache-to type=local,dest=<owned-directory>,mode=max`; a fresh
isolated builder on the same Machine must miss without import and hit with
`--cache-from type=local,src=<owned-directory>`. Repeat identical input on a
sibling Machine and a sibling Environment without import; require a miss.
Use fresh candidate-owned builders and retain vertex IDs and before/after cache
inventories; timing and payload hashes cannot substitute for cache observations.
Four concurrent builds and unrelated health probes still need harness scheduling.

`Dockerfile.cache` separately tests cache-mount contents using a fixed mount ID.
Supply owner, expected cold/warm state and distinct step tokens as in the manifest.
Changing the step forces a vertex to execute and inspect the actual mounted
sentinel. A foreign owner fails. Do not infer that exporting layer cache exports
cache-mount contents.

`Dockerfile.secret` requires `--secret id=fixture,src=<private copy of inputs/secret.txt>`
and its SHA-256 in `FIXTURE_SECRET_SHA256`. It compares bytes through the digest,
writes only the public success payload and checks the next RUN lacks the mount.
A missing-secret negative build must fail with a fresh builder or `--no-cache`.
Scan all image layers, decompressed exported cache blobs, logs, and public evidence
for the secret canary; merely scanning the merged final filesystem is inadequate.
Retain private fixture inputs separately from public evidence so the canary source
itself is not mistaken for leaked output. The intermediate marker is intentionally
a build-layer input; its exclusion assertion concerns the final stage only.

## SSH mount workload

Build the `ssh/` server image from the verified OpenSSH base. The future harness
creates a dedicated server on the exact Machine and a fresh isolated SSH agent;
it must never inherit the user's agent, keys, config, or known_hosts. Provision
a fresh private host key at `/run/secrets/host_key` (0600, root) and the test
agent's authorized public key at `/run/secrets/authorized_keys` (root-owned).
Use exact-container `docker cp` before first start, or a declared Machine-local
secret projection. Do not assume a host path passed to a remote bind mount is
automatically transferred into the guest. Retain only public key fingerprints
in public evidence; key bytes and server writable layers require exact cleanup.

The server accepts only public-key root authentication, forbids forwarding and
TTY, and forces the deterministic response command. The harness must make this
owned server reachable from the selected BuildKit worker's declared network.
There is no Internet SSH target, wildcard **host** publish, insecure host-network
entitlement, or NAT alias shortcut prescribed here.

Supply verified `FIXTURE_SSH_HOST`, port, a strict known_hosts secret derived
from that exact server key, and `--ssh fixture=<private agent socket>`.
The positive Dockerfile must return the exact response. The negative Dockerfile
has the same SSH command without an SSH mount and must fail authentication.
Also omit the required mount from the positive Dockerfile and test a wrong host
key. Disable build cache for negative tests. Scan exports/logs/evidence for the
private key, and bind every connection to the owned server identity.
The SSH daemon/base provisioning and network setup remain unverified.

## Compose workloads

Build `compose/Dockerfile`, retain its immutable image ID, then supply
`FIXTURE_IMAGE` and a distinct `FIXTURE_OWNER` for every Machine/run.
Pass `-f compose/compose.json` and a unique run-owned `--project-name` explicitly.
JSON is used as a YAML-compatible Compose document so offline validation needs
no YAML dependency. No service has a fixed container name, host port, runtime
override, or external/global network or volume name.

The database owns the state volume. API and workers each check the preceding
dependency's exact health response at startup. Compose gates both edges on
`service_healthy`; the host harness must also retain Engine health/start events
and prove the ordering, not rely on the workload's log assertion. The database
never repairs a foreign or corrupted sentinel. API `/value` reads the database
over its network; workers reach API but not database. Probe forbidden paths both
by DNS name and inspected actual container IP, recording every attempt.

Use `blocked-health.json` as a second Compose file in a separate fresh
project/volume: the database remains unhealthy and API/workers must not start.
Readiness has a 30-second deadline, one-second samples and no scenario retry.
The 2-second HTTP timeout bounds each workload request. Capture failure and
partial resource inventory, then perform exact project cleanup.

Scale worker to three and back to one. Inspect all IDs; each /identity hostname
must match that specific container's inspected default hostname. The host
harness must target every replica, not accept three requests to one load-balanced
name. Logs are ordered **per service instance and stream**, not globally across
replicas. Repeated lifecycle starts produce another exact startup sequence.

`compose exec --no-TTY api python3 /fixture/service.py exec` returns separate
exact stdout/stderr and exit 37. The optional `failure` profile runs a job that
returns 37 only after API health; use `up --abort-on-container-exit
--exit-code-from failure` and retain the failing job plus all partial resources.
Normal up excludes this profile. Down must remove only captured project-owned
resources; unrelated decoys, cross-Machine state and recovery schedules still
need the full harness.

Reference semantics: [Dockerfile mounts](https://docs.docker.com/reference/dockerfile/#run---mount),
[Compose services](https://docs.docker.com/reference/compose-file/services/),
and [local BuildKit cache](https://docs.docker.com/build/cache/backends/local/).
