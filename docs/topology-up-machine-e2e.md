# DEV physical public-CLI lifecycle runner

`scripts/run-topology-up-machine-e2e.sh` runs the sole opt-in test in
`crates/vz-cli/tests/topology_up_machine_e2e.rs` against explicitly staged signed
release `vz` and `vz-runtimed` files. It builds the release-profile **test driver**
with `--locked`; it does not rebuild, resign, install, or replace the selected
product files. Their directory must be outside Cargo's build-output directory.

On Apple-silicon macOS, with the verified normal Developer and Hardened bundles
already present in `linux/out` and `linux/out/container`:

```sh
bash scripts/run-topology-up-machine-e2e.sh \
  --release-dir /absolute/staged-signed-release \
  --docker /usr/local/bin/docker \
  --evidence-dir /absolute/existing-parent/new-owned-evidence
```

The release directory must contain exactly selected `vz` and `vz-runtimed`
regular executable files at its root (other evidence files may coexist). No
PATH lookup or old-installation fallback selects either product or Docker.
The explicit Docker invocation may be a symlink: its canonical executable is
hashed and passed to the driver, which uses `argv[0] = docker` for multicall
installations. Both original and canonical paths are recorded.

Unexpected, abbreviated, repeated or missing flags fail before evidence creation.
Existing evidence paths are never adopted or deleted. The parent must exist;
the new evidence root and its initially empty `physical/` subdirectory are mode
`0700`. Build/run stdout, stderr, argv, exits, timing and wrapper receipts live
outside `physical/`; that subdirectory belongs solely to the Rust driver.

The runner selects one exact integration-test executable from Cargo's JSON
messages, stages and hashes it, and invokes only
`installed_public_up_exec_stop_machine_lifecycle` using `--ignored --exact`.
Success requires exactly one passed test, zero failed/ignored/filtered cases,
and the driver's successful exact Stop cleanup plus clean daemon exit. There
are no test retries. Input hashes are checked before and after execution.
`inputs.sha256` binds current harness source, Cargo inputs, selected binaries and
Docker; `checksums.sha256` binds all generated nested evidence and the staged
test executable. Failed evidence remains available.

This is **DEV physical lifecycle evidence**, not Docker compatibility or the
0.4 aggregate. The current Rust scenario boots two Developer Machines to their
honest missing-conformance boundary, probes distinct Engine IDs from host Docker,
and checks Hardened Up/Exec/Stop/re-Up with exact streams, exit codes, a backing
store sentinel and unaffected sibling Engines. Expected missing Developer
conformance is not Developer readiness. The normal bundle verifier records the
actual kernel/initramfs/youki identities. Full host Docker/Compose/buildx,
workspace policy, mixed native-macOS topology, interrupted partial startup and
the aggregate release gate remain separate requirements.

The Rust driver owns exact Stop cleanup. Uncertain resources, its private runtime
root and any necessary live daemon are retained for reconciliation; this shell
runner never attempts broad cleanup or guesses process/resource ownership.

## Recorded development run

`.artifacts/topology-up-physical-backing-store/` passed on the local Mac against
the signed artifacts in `.artifacts/topology-cli-installed-7zd8qA/`. The result
and checked nested hashes cover the exact streams/exit, backing-share mount and
sentinel, stable IDs, six host Engine identity probes, endpoint removal, positive
Stop receipts and clean daemon shutdown. It does not certify Developer Ready.
Prior failed candidates `topology-up-physical-d8zdOX` (CLI stderr banner) and
`topology-up-physical-raw-streams` (incorrect fixture backing-store path) remain
retained with successful Stop cleanup; the passing candidate contains fixes.

Offline wrapper checks (no Docker or VM execution):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/helpers -p test_topology_up_machine_runner.py
bash scripts/run-topology-up-machine-e2e.sh --help
bash -n scripts/run-topology-up-machine-e2e.sh
```
