# 0.4 CLI removal and retained-spelling migration inventory

Status: normative 0.4 migration input

The 0.4 public CLI contains the lifecycle verbs `up`, `exec`, `status`, `stop`,
and `delete`, plus conventional read-only `--help` and `--version` and common
output controls such as `--json`, `--quiet`, and verbosity. This file freezes
the pre-0.4 root surface that the release gate must test. Adding or discovering
another legacy parser path requires adding it here before certification.

## Removed root commands

The v0.3 root commands below must not appear in normal or hidden 0.4 help and
must never execute. Each invocation returns the stable legacy-command error
class plus structured migration guidance:

```text
create
ls
rm
inspect
attach
close-shell
init
run
logs
stack
image
diff
checkpoint
vm
self-sign
debug
```

This removes every nested path beneath those roots, including legacy `build`,
Docker, OCI, sandbox, network, Machine/VM, image, checkpoint, patch, and debug
operations. The test enumerates nested help recursively from the pinned v0.3.20
binary and proves no formerly executable leaf remains reachable in 0.4.

## Removed bare-mode mutations and flags

Bare `vz` prints static top-level help, exits zero, and performs no state
discovery or mutation in 0.4. The implicit create/attach/continue/resume path
and these root mutation/configuration flags are removed:

```text
-c, --continue
-r, --resume
--name
--ephemeral
--cpus
--memory
--base-image
--main-container
--control-plane
```

Their functionality moves to `vz.json`, an explicit lifecycle selector, or a
typed Admin API. Passing one returns a stable structured migration error and
creates no resource.

## Retained spellings with replaced semantics

`status` and `stop` retain their words but not their v0.3 single-VM semantics.
The release gate proves they operate on the selected Environment topology, obey
the new selectors and ambiguity rules, and cannot reach the old direct VM path.

`up`, `exec`, and `delete` are new product verbs. There is no alias from `exec`
to the legacy `run` implementation or from `delete` to prefix/name-based sandbox
removal.

## Gate rule

The checked-in machine-readable 0.4 CLI manifest mirrors this inventory. The
gate snapshots public and hidden help, recursively invokes every removed root
and nested leaf, exercises every removed root flag, and checks zero mutation.
A missed executable parser path, generic success exit, or unstructured error
fails the release.
