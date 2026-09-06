# Machine-owned Docker client credentials

Status: **DEV**, implementation and focused verification in progress under
`vz-mzs.7.1.9`. Installed registry/login, restart/recovery and Delete acceptance
remain required; neither the image round-trip slice nor this document closes
the full Docker or 0.4 release gate.

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

Locally signed release artifacts pass 31 installed control-plane tests across
eight drivers in `.artifacts/topology-cli-installed-BWHPhd`. These are not VM
workloads or a GA-signed distribution. New physical public-Up/recovery/Delete
and authenticated-registry acceptance remain open.
