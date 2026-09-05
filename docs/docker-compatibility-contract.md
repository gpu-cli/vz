# Docker 0.4 compatibility requirement inputs

[The contract](../config/docker-compatibility-v0.4.json) enumerates all 63
minimum Docker scenarios in the [0.4 release goal](../planning/developer-environments/GOAL-0.4.0.md#strict-e2e-release-gate).
It is **draft, unverified requirement input**, not a compatibility report. There
are no exclusions, approvals, passing scenarios, or certified client versions.
BuildKit SSH mounts and Compose scaling remain required.

The candidate client pins are the locally observed Docker CLI 29.4.0
(`9d7ad9f`), Compose 5.1.2, and buildx 0.33.0
(`f7897eba028583e0071642db3c011e860444f8cf`). The retained observation contains
only three host version commands; it does not prove daemon connectivity or any
Docker behavior. Candidate versions are single-version ranges, not a claim that
untested older or newer versions work.

The pinned Engine is 29.7.2 (`6a43e3d`). Its default advertised API range is
1.40–1.55, verified against immutable
[Moby source](https://raw.githubusercontent.com/moby/moby/6a43e3d5afddf4111da0f864bbc7cae5d7e95001/daemon/config/config.go).
The upstream configurable historical floor, 1.24, is recorded separately; it is
not the default advertised minimum, and this contract disallows overriding that
minimum. A physical run must still verify the actual server range and
negotiation behavior.

## Validate the inputs

The helper needs the pinned `jsonschema` dependency:

```sh
uv run --with jsonschema==4.23.0 python scripts/helpers/docker_compatibility_contract.py --check-draft
uv run --with jsonschema==4.23.0 python scripts/helpers/test_docker_compatibility_contract.py
```

`--check-draft` checks structure, the complete scenario inventory, fixed expected
outcomes, client and isolation requirements, and the digest of the goal's exact
minimum-behavior section. It reports unresolved pins, zero Docker tests executed,
and `compatibility_certified: false`. Its zero exit status is **not a release
gate result**.

The default command is the fail-closed requirement-input preflight:

```sh
uv run --with jsonschema==4.23.0 python scripts/helpers/docker_compatibility_contract.py
```

It currently exits nonzero: the contract is not frozen, and both the complete
fixture bundle and executable host-client harness are pending. Do not use draft
lint in place of this preflight. Even a future successful preflight means only
`requirement_inputs_valid`, with zero Docker tests executed and no certification.
The helper never invokes Docker, fixture workloads, or the harness.

The [JSON schema](../schemas/docker-compatibility-v0.4.schema.json) fixes the
canonical scenario objects and rejects unknown fields, missing or duplicated
scenarios, weakened outcomes, new exclusions, and fabricated result fields.
Changing the normative minimum section requires an explicit catalog review;
changing the manifest alone cannot silently change the accepted requirements.

## Finish the executable lane

Each scenario has a stable ID, expected assertions, a selected Environment and
Machine scope, a phase, required raw evidence, and host command families.
`command_paths` are command prefixes, **not runnable command lines**. The full
harness must supply concrete fixture arguments, use the exact Machine's Docker
context, and retain the complete actual argv, stdout, stderr, exit status, timing,
owner/runtime/endpoint identities, artifact digests, and before/after inventories.
Values beginning with `fixture.` refer to deterministic expected values that
must be implemented in the still-pending fixture bundle; they are not observed
results or an excuse to accept arbitrary output.

The next implementation must:

1. Implement every scenario using the supported, unmodified host Docker,
   Compose, and buildx clients against each required Machine topology. Preserve
   the clean-provision, persisted-recovery, and final-cleanup phases and the
   goal's pressure, deadline, isolation, and exact-cleanup requirements.
2. Complete `tests/fixtures/vz-0.4/docker`, including deterministic payloads and
   expected values. Freeze its inventory digest, then set its pin to `pinned`.
3. Implement the complete `scripts/run-linux-docker-e2e.sh` lane, retain raw
   scenario receipts, and make the aggregate gate independently reject missing,
   skipped, retried, misrouted, stale, or failed evidence. Pin the executable's
   SHA-256 and set its state to `pinned`; freeze the contract only after both
   required inputs actually exist and validate.
4. Run that lane on the local Mac against the exact installed release artifacts,
   retaining reproducible evidence. Integrate it with the broader release gate;
   this Docker contract does not replace native-Machine, API/CLI, upgrade,
   networking, ownership, or other required acceptance lanes.

Fixture inventory hashing is SHA-256 over compact UTF-8 JSON of sorted
`[relative_path, POSIX_permission_mode, byte_length, content_sha256]` file rows.
The helper provides `fixture_tree_digest` for this exact calculation. Directory
entries are traversed without following links; symlinks, special files,
hardlinks, empty trees, changing inputs, and unbounded inventories are rejected.
The harness digest is SHA-256 of its regular, single-link executable file bytes.
The aggregate release evidence must additionally bind all transitive harness
helpers and release inputs; a shell entry-point digest alone cannot certify them.

No guest substitute client, internal Rust shortcut, mock, Docker Desktop daemon,
host-system daemon, global fallback socket, alternate OCI runtime, or runtime
override is acceptable. Exact per-Machine ownership and sibling-Environment
isolation remain mandatory throughout execution and cleanup.
