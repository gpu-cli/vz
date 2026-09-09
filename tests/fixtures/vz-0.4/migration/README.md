# Pinned v0.3.20 migration fixture

`v0.3.20-state.db` is the legacy state store the 0.4 release gate upgrades in
criterion 19 (`gate.migration.install_upgrade_rollback_uninstall`). Its digest
is pinned as `migration.legacy_state_fixture_sha256` in
`config/vz-0.4-e2e-contract.json`; the binary it was produced by is pinned as
`migration.legacy_artifact_url` / `legacy_artifact_sha256`.

## What it is

A SQLite state store at legacy `schema_version = 1` holding three
`sandbox_state` records, one of each classification the 0.4 migration
distinguishes:

| `sandbox_id` | classification | marker labels |
|---|---|---|
| `vz-run-proj-1fb3d4946b46` | Developer | `vz.run.workspace=/workspace` |
| `vz-space-hardened-shop-0a1b2c3d4e5f` | Hardened | `vz.space.mode=required` |
| `vz-legacy-generic-tool-9f8e7d6c5b4a` | generic | neither marker |

`crates/vz-runtime-contract/src/types/topology.rs::migrate_legacy_developer_sandbox`
migrates the Developer record and refuses the other two with `NotDeveloper`, so
`crates/vz-stack/src/state_store/topology.rs::migrate_legacy_v1_to_v2_with_hook`
leaves their legacy rows exactly as v0.3.20 wrote them. The store also carries
the events, receipts, execution and container rows the daemon produced while the
Developer sandbox ran, so migration is exercised against a populated store
rather than an empty one.

`project/` is the legacy project the Developer record belongs to: the v0.3.20
`vz.json`, a `sentinel.txt` written before the upgrade, and
`legacy-payload.txt`, which the legacy VM itself wrote through the workspace
mount. Uninstall must leave all three byte-identical.

## Provenance

Everything in `v0.3.20-state.db` was written by the pinned v0.3.20 release
binaries, downloaded from the v0.3.20 GitHub release and verified against the
digests recorded in the contract:

1. `vz-runtimed-v0.3.20-darwin-arm64` was started over an empty state store with
   `--state-store-path`/`--runtime-data-dir`/`--socket-path` inside a disposable
   `HOME`, with the pinned `vz-linux-developer-v0.3.20-arm64.tar.gz` bundle
   staged at `$HOME/.vz/linux`.
2. `vz-v0.3.20-darwin-arm64 run` booted a real Linux VM from the v0.3.20
   `vz.json` and wrote `legacy-payload.txt` through the workspace mount. That
   produced the Developer `sandbox_state` row in state `ready`, along with its
   events, receipts, execution and container rows.
3. The daemon was stopped with `SIGTERM` so the store checkpointed its WAL and
   the shipped file is complete on its own.
4. The Hardened and generic rows were then inserted into the same store in the
   `sandbox_state` shape v0.3.20 defines. v0.3.20's spaces mode, which is what
   produced Hardened records in the field, refuses to run on macOS
   (`spaces mode requires Linux btrfs workspace storage`), so those two rows
   could not be produced by running v0.3.20 on this host.
5. A **copy** of the finished store was opened by the v0.3.20 daemon again and
   `vz-v0.3.20-darwin-arm64 --json ls` enumerated all three records, decoding
   each one's backend, spec and labels — including the label normalisation
   v0.3.20 applies to a Hardened record's `main_container`. That is the evidence
   the two inserted rows are v0.3.20 records and not a stand-in shape. The
   verification ran on a copy so the shipped bytes are exactly those step 4 left.

Re-running the upgrade against this fixture must never mutate it: the gate check
copies it into a disposable state root first.
