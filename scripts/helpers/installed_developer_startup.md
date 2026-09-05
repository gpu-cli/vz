# Installed Developer operational startup — DEV physical proof

This separate harness leaves the older expected-failure lifecycle proof unchanged.
It does not certify the full Docker compatibility contract or the 0.4 release.

Run only after the selected signed release binaries and **normal** Developer and
Hardened Linux profile builds have completed. The helper does not rebuild them,
download images, use a global Docker daemon, or modify the daily installation.
All flags are required; use canonical absolute input directories and a new
evidence directory whose canonical parent already exists:

```bash
scripts/run-installed-developer-startup-e2e.sh \
  --release-dir /absolute/staged-signed-release \
  --release-version 0.4.0-dev \
  --developer-bundle /absolute/vz/linux/out \
  --hardened-bundle /absolute/vz/linux/out/container \
  --docker /usr/local/bin/docker \
  --compose-plugin /Applications/OrbStack.app/Contents/MacOS/xbin/docker-compose \
  --buildx-plugin /Applications/OrbStack.app/Contents/MacOS/xbin/docker-buildx \
  --evidence-dir /absolute/new-evidence
```

The physical sequence is:

1. Copy exact signed `vz` and `vz-runtimed` into a fresh private installation's
   `bin/`; verify signatures and hashes. Copy selected digest-verified bundles
   into `linux/developer` and `linux/container`; generate the installed catalog
   through the daemon's offline installer entry point.
2. Copy pinned Compose/buildx executable bytes under their proper applet names
   in an isolated Docker config. Preserve `HOME` unchanged. Clear inherited
   Docker routing, proxy, Cargo-daemon, SSH-agent and catalog overrides. Only
   explicit runtime/state/socket and host-client/config isolation overrides
   enter the product invocation.
3. Run normal public `vz up` from an owned Git worktree. Let that CLI discover
   its sibling daemon and the installation-prefix catalog. No direct daemon
   startup, catalog environment variable, substitute API or legacy verb.
4. Create primary and neighbor named Environments from the same unmodified
   two-Machine project definition: **four concurrent Developer Machines**,
   each with 4 GiB RAM. Require successful Up, ready status, all three negotiated
   capabilities, exact owned context descriptors and distinct Engine IDs.
   Require exactly two Machines in each Environment, the same Project ID,
   distinct Environment IDs and four distinct Developer Machine IDs.
5. Independently drive the unmodified host clients through those contexts:
   immutable rootfs import, container run/exec, Compose single-service exec,
   Buildx `FROM scratch`/`COPY` build, then copy and compare the actual image's
   public payload. No mutable base image, plugin bootstrap, or daemon fallback.
6. Stop primary, prove neighbor remains usable, Up primary again, and compare
   stable Environment/Machine/context identities and advanced incarnations.
   Stop both Developer Environments; boot a sequential Hardened Machine, require
   no Docker descriptor/capabilities, and execute a raw-stream marker command.
7. Require positive public Stop receipts for all admitted Environments and
   absent private Machine sockets. Authenticate the autospawned daemon by its
   owned PID file, exact executable path/hash, UID, process start time and
   isolated arguments before SIGTERM. Require its PID/socket removal, process
   disappearance and positive graceful-shutdown log. Never force-kill a daemon.

Every command gets a durable intent before dispatch, raw bounded stdout/stderr,
exit/timing receipt and hashes. Multicall Docker uses canonical executable bytes
with logical `argv[0]=docker`; plugin copies retain `docker-compose` and
`docker-buildx` filenames. Interrupted, timed-out or output-limit commands leave
explicit uncertainty and withhold automated lifecycle cleanup. Failed normal
commands abort the scenario; their known Environment targets are stopped when
host dispatch certainty allows it. Unknown resources are never pruned.
Even a nonzero Up exit is parsed for authenticated streamed admission first;
cleanup uses its immutable Environment ID, never the original name. An Up with
no authenticated admission withholds automated Stop and daemon termination.
Host Docker/plugin commands own a bounded process group. A timed-out public CLI
observer is killed/reaped **by its own PID only**, because its normally
autospawned daemon inherits the CLI group; that daemon is retained for exact
reconciliation, never incidentally SIGKILLed with an observer.

The new installation, Git fixture, VM data, context configuration and runtime
receipts remain under the exact short `/private/tmp/vzdev-*` root recorded in
`layout.json`, even after success. Nothing is recursively deleted. Raw command
logs and bounded runtime journals are copied into the evidence directory with a
checksum manifest. `result.json` reports success only when all observations and
cleanup checks pass. Process exit status of the detached daemon cannot be waited
by this harness; the receipt explicitly proves observed graceful termination,
not a fabricated child exit code.
The shutdown log is read through a capped, no-follow, private single-link regular
file descriptor with before/after identity checks.

Offline prerequisite checks (do not execute Docker or VMs):

```bash
/usr/bin/python3 -m unittest discover -s scripts/helpers -p test_installed_developer_startup.py -v
scripts/run-installed-developer-startup-e2e.sh --help
```
