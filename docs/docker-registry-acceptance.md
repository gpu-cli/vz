# Installed registry acceptance

Status: **DEV**, in progress under `vz-mzs.7.1.10`. The three required catalog
cases are `docker.image.registry_login`, `docker.image.pull`, and
`docker.image.push`. An explicit `--suite registry` dispatcher now exists and
one installed local-Mac candidate has passed it (below). This is a DEV slice
pass, not a release certification: Docker-63 and the full 0.4 aggregate remain
open, and `--suite all` still rejects incomplete coverage.

## Installed registry candidate 6 (2026-09-07)

`scripts/run-linux-docker-registry-e2e.sh` (isolated `uv` invocation of the
pinned Python dependencies; no global install) with the installed
`.artifacts/topology-cli-installed-BVlJY5` CLI/daemon (source commit
`17a0a442`, not HEAD), the `20260906T210423Z` developer/container guest bundles,
OrbStack's Docker CLI 29.4.0 under Machine-private `--config`/`--context`, the
pinned `registry-3.1.1-load-candidate-1.tar` and the admitted
`registry-3.1.1-bounded-replay-1/layout`. Evidence:
`.artifacts/linux-docker-registry-candidate-6` (1,480 files, `checksums.sha256`
verifies), outcome `passed_dev_installed_registry_slice`, zero retries.

Two Environments × two Linux Machines; the registry ran on three selected
Machines at the same private authority `172.30.241.2:5443`, the fourth Machine
stayed a neighbor sentinel. Per selected Machine, in order and exactly once:
fixture load, private guest setup/admit, owned `--internal` network/volume/server,
startup-log identity (five JSON rows, one instance id, `listening on
172.30.241.2:5443, tls`), wrong-CA login rejected with the exact daemon x509
error and exactly one `remote error: tls: bad certificate` handshake line,
owned CA trust install, invalid-password login `401 Unauthorized` with
credentials unchanged, unauthenticated push denied with storage inventory
unchanged, valid login (exact stdout/stderr, only the selected config gained an
auth entry), server-side route witness (one authorized `GET /v2/` by the guest
Engine 29.7.2 User-Agent with the upstream CLI suffix, from the bridge
gateway, inside a same-guest clock window), authorized push, remote
manifest/config/layer bytes validated, local removal, pull-by-digest, export
re-verified byte-for-byte, independent receipt replay and secret-canary scan
(313 files per Machine, no canary in raw/base64/hex/auth-blob forms), logout,
final log classification (exactly two blob and one manifest 404 push probes),
exact cleanup with guest CLEANUP acknowledgment. Cross-Machine controls proved
four distinct config directories, siblings/sentinel/host defaults/credential
helpers/shared plugins unchanged at every phase, all four empty at the end.

Candidates 1–5 failed closed on source-based expectations that the real
Engine/Distribution pair corrected (empty endpoint gateway on `--internal`
networks, `version` logged as `3.1.1`, htpasswd `username`/`error` keys,
containerd's `distribution.source` index annotation, 404 push probes). Each
was dispositioned by exact positive-inspection cleanup, public Stop and
graceful daemon termination (`linux-docker-registry-candidate-disposition.py`);
their runtime roots and evidence are retained.

Candidate 7 (`.artifacts/linux-docker-registry-candidate-7`) repeated the
identical pass with a CLI/daemon built and signed from HEAD `c019b4a0`
(`.artifacts/topology-cli-installed-yp2Cum`, clean tracked state, all eight
control-plane drivers passing) and the same guest bundles.

Not closed by this pass: the literal raw `/auth` route witness (see "Route
witness boundary"), `vz-mzs.7.1.9` credential recovery/migration across
Stop/Up, Docker-63 and the 0.4 aggregate.

The earlier checkpoints below are historical.

## Frozen public fixture

`config/docker-registry-artifact-v3.1.1.json` pins the real Distribution 3.1.1
Linux/arm64 image: upstream multi-platform index, selected manifest, config,
five compressed layers, their decoded diff IDs, and a selected-only OCI layout
index. The selected manifest is
`sha256:bc68ba48dae0e0423bb885c8d07d20c3210febbe996d38d54d32c574fda690ae`.
The version source is the [official registry image definition](https://github.com/docker-library/official-images/blob/master/library/registry);
its observed image-build commit is
[`0c2328319a030d953c159f57e4ad7693a9a08a8a`](https://github.com/distribution/distribution-library-image/tree/0c2328319a030d953c159f57e4ad7693a9a08a8a).
Reproduction uses checked-in digests, never that mutable source page or a tag.
These HTTPS-acquired pins are not a signature-verification or reproducible-build
attestation for upstream image contents.

Fetch into a new, explicitly selected artifact directory from the repository
root; this does not use Docker, host credentials, credential helpers, or
inherited proxies:

```bash
python3 -B scripts/helpers/linux_docker_registry_acquire.py \
  --pin config/docker-registry-artifact-v3.1.1.json \
  --output /absolute/new/registry-inputs
python3 -B scripts/helpers/linux_docker_registry_fixture.py \
  --pin config/docker-registry-artifact-v3.1.1.json \
  --layout /absolute/new/registry-inputs/layout
python3 -B scripts/helpers/linux_docker_registry_archive.py \
  --pin config/docker-registry-artifact-v3.1.1.json \
  --layout /absolute/new/registry-inputs/layout \
  --output /absolute/new/registry-inputs/registry.tar
```

The maintainer-only `--resolve-version` mode is explicit initial resolution, not
a fallback when immutable fetches fail. Anonymous public pull tokens remain in
memory; redirects do not forward registry Authorization to the CDN. Signed
redirect URLs are not retained. Acquisition bounds response sizes, redirect
count, socket inactivity and streaming deadlines. Failed partial directories
remain separate; acquisition does not itself verify decoded layer contents.

Offline admission separately verifies exact inventory, stable no-symlink paths,
all compressed hashes, and every decoded layer diff ID without extraction. It
does not claim Docker load or registry execution. The full upstream index is
retained as a provenance blob, but the loadable root index selects only arm64;
unselected platforms and attestations are not silently treated as downloaded.
The archive writer emits deterministic regular USTAR members and independently
replays every byte against the admitted layout before returning success. Use
the same archive command with `--verify-only` for a read-only replay; no helper
extracts the archive or invokes Docker.

## Private transport and route proof

`linux_docker_private_stdin.Capture` is a one-shot owner registered by the caller
before dispatch. It delivers bounded private bytes through pipes, records
complete write/EOF/exit only when proven, and retains uncertain state and any
pending process handle on failure. Public results contain only preselected,
exact acknowledgments. Unexpected output is withheld entirely, including encoded
credentials; no private plan, input hash, arbitrary exception text, or raw
unexpected stream is published. This is not secure memory erasure or independent
replay of secret input bytes. The private fixture must remain available until
the caller's independent acceptance checks complete.

The route verifier checks complete, bounded JSON stderr deltas from unmodified
Distribution, not hand-written server assertions. It requires matching
`authorized request` and `response completed` records, the exact Engine User-Agent
including the upstream Mac CLI identity, public fixture username, server instance,
request ID, and an independently bound same-guest command window.
This is source-attributed route inference, **not a direct captured `/auth`
request** or protection against a malicious same-owner process forging headers.
The [pinned CLI login code](https://github.com/docker/cli/blob/v29.4.0/cli/command/registry/login.go)
can fall back to host-side login on an Engine connection failure; successful
CLI output alone cannot establish the required guest path.

Do not enable generic dockerd debug logging to obtain this evidence. Use
Distribution's JSON logrus request records, with its separate combined access
logger explicitly disabled or separately retained; never silently filter lines.
Provision the exact private htpasswd file before server startup: missing-file
initialization can generate and log credentials. Unknown fields and fixture
canaries fail admission.

## Route witness boundary

Distribution 3.1.1's own per-request JSON records (`registry/handlers/app.go`
dispatcher, `internal/dcontext/http.go`) carry `http.request.method`,
`http.request.uri`, `http.request.useragent`, `http.request.remoteaddr` and
`http.request.id`. The verifier requires GET `/v2/` on every record, the exact
Engine 29.7.2 User-Agent with the escaped `UpstreamClient(Docker-Client/29.4.0
\(darwin\))` suffix, a `remoteaddr` on the owned bridge gateway, and paired
`authorized request`/`response completed` rows for the fixture user inside an
independently sampled same-guest clock window. This is a server-side request
witness emitted inside the registry process for the request the guest daemon
actually made, and it excludes the CLI's client-side fallback (whose UA is the
bare CLI UA and whose peer could not be the guest daemon on an `--internal`
network). It does **not** satisfy the issue's literal "raw `/auth` route
witness": nothing captures the CLI-to-daemon `/auth` HTTP call or the
daemon-to-registry TLS socket, so the proof reports
`direct_auth_endpoint_trace: False` and `wire_trace_captured: False`. What
remains is a daemon-side witness of the `/auth` request that does not leak
credentials (generic `dockerd` debug logging is unacceptable because it can log
tokens). Until one exists, acceptance rests on this server-side inference plus
the complete classified registry stderr (`startup-log.json`, per-phase
`*-log.json`, retained `registry-stderr-*.log`), where every non-JSON line is
accounted for as exactly one `remote error: tls: bad certificate` handshake
failure from the wrong-CA probe.

## Required installed integration

Each selected Machine receives one exact owned internal Docker network and
registry, using the same nonloopback private authority across isolated Machines.
No host-published port, NAT alias, host import or external registry write is
needed. Preflight must reject conflicting network/resources before mutation.
The private IP must have an exact certificate SAN and owned guest CA entry.
Loopback registries are unsuitable for the TLS-positive proof because the pinned
Engine's default insecure ranges include loopback. Keep insecure-registry
configuration unchanged and test wrong-CA rejection.

TLS keys, htpasswd and HTTP signing secret belong in bounded private input and
runtime-owned storage, never image layers, argv, environment or public plans.
Public TLS metadata validation only binds externally verified fingerprints,
authority and validity; it does not verify certificate signatures or a handshake.

The installed sequence must still implement and prove:

1. Invalid login leaves auth state unchanged; unauthenticated push fails for
   authorization and leaves registry inventory unchanged.
2. Valid login changes only the exact selected Machine config, with positive
   guest routing evidence and no default/helper/sibling credential changes.
3. Authorized push produces the independent tiny fixture's manifest/content;
   removal of only its local reference followed by pull-by-digest and export
   re-verifies every manifest/config/layer/payload byte.
4. Same-authority sibling/neighbor controls prove credentials do not cross
   ownership boundaries. Basic htpasswd proves missing/wrong-auth denial, not
   authenticated per-repository ACLs.
5. Independent replay, secret-canary scans and exact cleanup bind registry,
   network, volume, local references, CA and private input to their owner while
   preserving neighbors and host defaults. Stop/recovery credential persistence
   and migration remain required by `vz-mzs.7.1.9` and the wider release gate.

The generic Docker driver still rejects mutable credentials and public
interactive plans still retain their inputs. Neither should be weakened to
accommodate this lane: it needs explicit private-config admission and private
transport integration.

## Input checkpoint

The initial acquisition `.artifacts/registry-3.1.1-input-candidate-1` failed on
the previously unrecognized public Docker CDN hostname before downloading the
config/layers; its partial metadata remains retained. The corrected transport
admits that exact HTTPS hostname and tests stripping registry Authorization on
the CDN redirect. Candidate 2 and immutable-descriptor replays use separate
directories, not replacement evidence.

`.artifacts/registry-3.1.1-input-candidate-2/layout` and
`.artifacts/registry-3.1.1-bounded-replay-1/layout` independently pass complete
offline admission with identical inventories: 18,838,548 layout bytes and
60,035,072 decoded layer bytes. The inventory digest is
`c68fe6281d504e5fbbe496dbde5b03db48bf368872a8b5c7c7c1ffb9cb4b6ea8`.
The generated `.artifacts/registry-3.1.1-load-candidate-1.tar` passes separate
read-only replay: 18,847,232 bytes, ten regular members, SHA-256
`b971780e748c210e9b26ee86334a762160dedeccbada5beabf158d6b765cc9c3`.

Private-stdin tests include actual harmless Python pipe children, successful
fixed acknowledgment and complete withholding of an encoded-secret echo;
both children exit and are reaped. Registry route tests use source-shaped
synthetic logs, not observed registry execution. No Docker command or VM was
dispatched for this input checkpoint, and no registry scenario is closed by it.

The final selected Docker/installed Python family passes 1,046 tests in 35.899
seconds, including the new fixture, archive, private-stdin, acquisition and route
adversaries. Log `.artifacts/registry-foundations-python-family-final.log`, SHA-256
`77899859d491baf067a37b84a693ffdf413b17dbdc82beb6a084a3c6c2347d88`.
