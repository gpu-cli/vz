# Machine-owned Docker client credentials

Status: **DEV**, implementation and focused verification in progress under
`vz-mzs.7.1.9`. Installed public Up/Stop/Up and happy-path Delete pass;
actual registry authentication, daemon-restart credential persistence, migration
and fault acceptance remain required. Neither this slice nor this document
closes the full Docker or 0.4 release gate.

Every Developer Linux Machine owns a client configuration directory inside its
existing, authenticated runtime store. A separate immutable ownership claim
binds the logical Machine and directory identity; registry credentials are
mutable private contents, never ownership evidence. Initial publication is
create-only and atomic. Existing unclaimed directories, foreign claims,
symlinks, non-private files and inconsistent reads fail without adoption or
repair. Docker's atomic config-file replacements are permitted between coherent
reads. Stop and daemon recovery preserve the directory, claim and credential
bytes; exact-owner Delete removes this private subtree with the Machine store.

Use the `config_dir` **and** context `name` returned by `vz status`:

```bash
docker --config '<Machine config_dir>' --context '<Machine context name>' info
```

There is no global/default context change or new `vz docker` verb. Managed
product invocations remove inherited `DOCKER_*`, Compose, Buildx and proxy
overrides. Native clients invoked directly by a caller still obey that caller's
explicit environment, including `DOCKER_AUTH_CONFIG`; callers must not supply
foreign credential or routing overrides when selecting a Machine.

## Credential-store policy

A fresh directory alone does not prevent Docker from auto-selecting the Mac
keychain helper. The Machine config uses this exact credential-helper map:

```json
{"credHelpers":{"vz-managed-file-store.invalid":""}}
```

The reserved `.invalid` entry contains no credential and names no executable.
For the pinned Docker CLI, a nonempty helper map suppresses automatic native
store detection, while an empty helper suffix selects Docker's file store.
No ambient `credsStore`, registry helper, auth record, proxy or HTTP header is
copied. Managed invocations validate this policy before client dispatch.
The upstream behavior is defined by
[config loading](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/config/config.go)
and [credential-store selection](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/config/configfile/file.go).

This is Docker's ordinary **unencrypted file storage** inside a mode-0700
directory with mode-0600 files, not encrypted keychain storage. Auth values must
not appear in status, context descriptions, public evidence, logs or errors.
An installed-client check with an inert fake keychain helper confirmed that an
empty-config control invokes it, but the guarded configs do not. Logout of the
last synthetic auth record preserves the guard and default context; Docker may
omit an empty plugin-directory list when saving. This local-only check does not
establish registry authentication or guest Engine routing.

Plugin discovery is separate: the host client's discovery config may supply
its `cli-plugins` directory, which private configs reference without copying
`config.json`. Standard native plugin locations remain available. Arbitrary
settings or `cliPluginsExtraDirs` from an ambient config are not imported.
Changing source-selected plugin directories currently fails reconciliation
without rewriting credentials; explicit plugin-layout migration remains work.

## Legacy DEV claims and acceptance still required

Older DEV context claims name a shared host config. They cannot silently become
Machine-owned credentials. Up refuses those claims before creating private
client state, with migration guidance; Stop and exact legacy context Delete
remain available using the original configuration. Legacy Delete must retain
the shared config and its credentials. An explicit migration is not yet
implemented; this refusal does not satisfy the release migration gate.

Fresh installed acceptance must exercise two Environments with two Machines
each, same-registry credential separation, exact client/context/Engine routing,
unchanged host defaults/helper state, preserved Stop/Up and daemon-recovery
identity, and ownership-safe Delete while neighbors remain usable. Include
foreign paths/claims, symlinks, hard links, FIFOs, file replacement and partial
publication/deletion controls.

Registry cases additionally require bounded private password-stdin delivery,
secret-free public records and leak checks. A successful `docker login` is not
proof of guest-side authentication: the pinned CLI
[falls back to client-side registry login on an Engine connection failure](https://raw.githubusercontent.com/docker/cli/v29.4.0/cli/command/registry/login.go).
The lane must positively bind the guest `/auth` path and reject fallback.

## Verification checkpoint

The runtime unit suite passes 409 tests; four opt-in tests are not included in
that default run. Both installed-client offline context tests were separately
invoked and pass, including private context isolation, reopening and exact
context removal with zero Engine connections. Logs:
`.artifacts/managed-docker-config-runtime-unit-2.log` and
`.artifacts/managed-docker-config-native-context-1.log`.

The complete selected Docker/installed Python family passes 987 tests, including
checks that startup and pre-Delete collectors never read/copy private client
contents. Log `.artifacts/managed-docker-config-python-regression-4.log`, SHA-256
`b91abba7714c791cd9794710b82408e55b9234a1b412414fed0b0e2cec3f157b`.
Production-library strict Clippy and formatting pass. Strict all-target Clippy
still fails on test-target diagnostics; it is not reported as passing.

Locally signed release artifacts built from commit `17a0a442` pass 31 installed
control-plane tests across eight drivers in
`.artifacts/topology-cli-installed-BVlJY5`. CLI SHA-256:
`531308e1f7aa86430e217914317d8bb866f866e88fd2af1136c22662076bf287`;
daemon SHA-256:
`b44a38afcb3b2b88486cd66b675e20e74a500df373bc651d797e1f3e268b2d33`.
These are locally test-signed artifacts, not a GA-signed distribution.

Two fresh physical installed local-Mac runs using those artifacts passed and
were independently audited against their complete checksummed inventories:

- `.artifacts/managed-docker-config-startup-candidate-1`: normal public Up
  produced four distinct Developer Machine configs across two Environments.
  Docker, Compose and buildx workloads passed; primary Stop/Up preserved Machine
  config paths while advancing incarnation, without disturbing the neighbor.
  Hardened behavior and final clean guest Stops passed. All 1,135 evidence files
  were verified. Manifest SHA-256:
  `e72cbefce1cdb8e2c9f21ce71c243100f93b29feb765d205ed94e73ee9357937`;
  result SHA-256:
  `879cebd9a7641ef8c2fc8843e7aa0a726d86caedbbdc420274fd12e5f3e113f5`.
- `.artifacts/managed-docker-config-delete-candidate-1`: ready and stopped
  public Delete removed six exact owned Machine stores, including private client
  configs. Old-request replay returned the original tombstone and preserved the
  same-name replacement Environment. Neighbor checks and host files/defaults
  remained intact. All 4,164 evidence files were verified. Manifest SHA-256:
  `6eb9f88e1f020b49a11574ebe4718c7fe0108603ffff1afe9f545fecc3ab6c91`;
  result SHA-256:
  `48fab46e692e24b143f4c7872e0398eee23bca1d93364587274e208ca73c1931`.

Delete continuity is sampled, not a zero-downtime claim: ready Delete had 51
overlapping samples covering both neighbors; the fast old-request replay had
none, and stopped Delete had one covering only one neighbor. Each operation
also had positive checks for both neighbors before and after. Both run daemons
exited normally; their sockets and PID files are absent. Public collectors
excluded private client contents. Startup disks/configs remain retained after
Stop; the six disposable Delete stores were removed and require fixture
recreation to recover their contents.

Neither run performs a real registry login or proves credential persistence
through daemon restart, migration, or fault recovery. Authenticated TLS registry
login/push/pull is tracked in `vz-mzs.7.1.10`; its
[fixture, private transport and acceptance boundaries](docker-registry-acceptance.md)
must supply the remaining actual
credential-isolation evidence for `vz-mzs.7.1.9`. Both that issue and the full
Docker-63/0.4 aggregate remain open.
