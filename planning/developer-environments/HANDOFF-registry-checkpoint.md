# Continuation: installed registry checkpoint

> **Superseded 2026-09-07.** The work below was resumed: the Session log seam is
> fixed, `--suite registry` is wired (`scripts/run-linux-docker-registry-e2e.sh`),
> cross-Machine credential controls, independent replay and canary scans exist,
> and installed candidate 6 passed the DEV registry slice on three Machines
> (`.artifacts/linux-docker-registry-candidate-6`). See
> `docs/docker-registry-acceptance.md` for the current status and remaining
> work; the numbered list below is retained as history, not instructions.

This is an unfinished DEV checkpoint, not a 0.4 release or a registry acceptance
pass. The user requested a handoff because of usage limits. Do not resume an
assumed physical run: this checkpoint dispatched no Docker command or VM.

Publication integrated origin `b878a29a` (native-macOS bootstrap PR #3, 19 new
commits) by a clean rebase. Those changes do not overlap the registry files and
were not runtime-tested by this checkpoint. Re-read the updated product/goal and
native-macOS planning/evidence documents: this handoff's earlier artifact records
must not be mistaken for the latest native lane status. The 1,137-test result
below was obtained before the rebase; it is not verification of incoming Rust.

## Start here

Read `AGENTS.md`, `docs/developer-environments.md`, this directory's
`GOAL-0.4.0.md`, `docs/agent-verification.md`, and
`docs/docker-registry-acceptance.md`. Use the implement-bd skill and subagents
(the user explicitly requested them). Inspect current git and Beads state before
editing. The complete goal is also retained locally at
`/Users/jameslal/.codex/attachments/92e3c5d3-c157-4537-99bd-761fb27d6cd7/pasted-text-1.txt`.

The outcome is one installed, release-built, local Apple-silicon Mac aggregate:
clean provision → persisted recovery → final cleanup, every required scenario
exactly once, zero skips/retries/fallbacks/leaks/digest mismatches, one release
digest and independently replayable evidence. Focused tests do not close it.

Tracked work:

- `vz-mzs.7.1.10`: registry login/push/pull; in progress, current focus.
- `vz-mzs.7.1.9`: Machine-private Docker client credentials/configuration;
  implementation and focused lifecycle evidence exist, but actual authentication,
  credential recovery and migration remain.
- `vz-mzs.7.1.8`: closed four-case image slice.
- `vz-mzs.7.1.6`: lifecycle slice still needs runtime causality, real errno
  controls and recovery audit coverage despite focused command passes.
- `vz-1ff`: strict all-target lint restoration, still open.
- Parent Docker-63 and the full 0.4 gate remain open. `--suite all` deliberately
  rejects incomplete coverage. Do not remove that guard to manufacture a pass.

## What this checkpoint adds

All helpers below are in `scripts/helpers/`, with matching unit tests:

- `linux_docker_registry_credentials.py`: immutable owner/directory admission,
  private snapshots and exact empty↔login config transitions, without public auth
  bytes or hashes. Rejects ambient helpers and foreign config settings.
- `linux_docker_registry_secrets.py`: in-memory native-verified CA/server keys,
  exact IP SAN, distinct per-Machine passwords, bcrypt and Distribution config.
  Private material only travels through bounded private stdin. Distribution has
  explicit `http.draintimeout: 5s` for graceful stop.
- `linux_docker_registry_guest.py`: exact BusyBox setup/admission/trust/cleanup
  scripts, owned private files and CA directory; fixed private acknowledgments.
  Scripts have not yet been exercised in the actual guest.
- `linux_docker_registry_commands.py`: registered-before-dispatch private command
  ownership, fixed acknowledgments, durable receipts, pinned executable and
  descriptor checks, fail-closed uncertainty handling.
- `linux_docker_registry_image.py`: independent tiny-image load/tag/push/remove/
  pull-by-digest/export recipe and byte validators.
- `linux_docker_registry_binary.py`: read-only admission of actual registry
  binary/build metadata from pinned OCI layers, without extraction or execution.
- `linux_docker_registry_session.py`: provisional assembly for prepare, wrong CA,
  wrong password, unauthorized push, valid login, roundtrip and exact cleanup.
  This is NOT wired into the installed runner. Its tests cover construction,
  identity, private-login seams and early failures, not a real full session.
- `registry-requirements.in` and hash-bearing `.txt`: isolated Python fixture
  dependencies. Do not install into system Python or change the user's defaults.

Private stdin's per-argument bound increased to 16 KiB for public guest scripts;
environment values remain limited to 4 KiB and total public input to 64 KiB.
No production Rust behavior changed in this checkpoint.

Verification: the complete selected Python family passed **1,137 tests in
39.797 seconds**, exit 0, using the pinned requirements and system Python 3.9.
Expected argparse rejection diagnostics appear in the negative tests. The test
process was observed complete and reaped. Subagent checks additionally passed
16 native crypto tests, 17 Session tests, 12 binary tests, and actual offline
pinned binary admission. `git diff --check` passed. None is a physical registry
compatibility pass.

Re-run the family from the repository root:

```bash
uv run --no-project --python /usr/bin/python3 \
  --with-requirements scripts/helpers/registry-requirements.txt \
  python -B -c 'import unittest; suite=unittest.TestSuite(); loader=unittest.TestLoader(); [suite.addTests(loader.discover("scripts/helpers",pattern=p)) for p in ("test_linux_docker_*.py","test_docker_*.py","test_installed_*.py")]; r=unittest.TextTestRunner(verbosity=1).run(suite); raise SystemExit(not r.wasSuccessful())'
```

## Immediate next work, in order

1. Fix the known Session log seam before any physical dispatch. `authenticate()`
   currently parses ALL pre-valid-login stderr as JSON to derive `instance.id`.
   The wrong-CA probe can generate a Go standard-library plaintext TLS handshake
   error. Capture and validate the instance from startup JSON in `prepare()`
   BEFORE that probe; retain complete raw logs and keep the successful-login
   delta strictly validated. Do not silently filter unknown non-JSON rows.
2. Resolve the route-evidence acceptance gap explicitly. The existing verifier
   proves source-attributed guest Engine/upstream CLI route inference from
   Distribution logs, NOT a directly captured `/auth` request. The Beads design
   asks for a raw route witness. Do not silently equate the two or weaken the
   criterion. Generic dockerd debug can leak tokens: do not enable it.
3. Wire an explicit `--suite registry` into `linux_docker_e2e.py` (prefer a
   separate Machine-scenario module). Add preflight artifact/layout/archive/
   binary/dependency/source-closure admission before mutation. The shell wrapper
   currently hardcodes `/usr/bin/python3`; provide an explicit isolated dependency
   invocation, not a silent global install.
4. Use two Environments with two Linux Machines each: run the registry on three
   selected Machines, keep the fourth as a neighbor sentinel. Authenticate all
   exact descriptors and prove all four Docker config directories distinct.
   Same authority `172.30.241.2:5443` is intentional across isolated Machines;
   each has distinct secrets. Add same-authority sibling/neighbor credentials
   controls and unchanged host/default/helper state checks.
5. Review and test all Session phases before runtime: exact CLI diagnostic
   acknowledgments are source-based expectations, not yet observed. Strengthen
   complete before/after image/resource inventories and cleanup proofs. Preserve
   the manifest ID assertions: this pinned Moby/containerd store uses manifest
   target IDs, not config digests, including container Image identity.
6. Integrate private owner certainty and cleanup into harness finalization.
   `ComposeHarness.assert_certain()` is a FINAL cleanup guard requiring a stopped
   monitor; do not call it during a live workload. Session.certain deliberately
   uses narrower checks. Register all private captures before dispatch and
   require terminal/reaped receipts and each Session's cleanup completion before
   normal sentinel removal/Stop. Failed or uncertain effects do not authorize
   blind cleanup, force removal, retries or VM respins.
7. Implement independent raw-command replay, secret-canary scans (including
   logs/cache/export) and exact resource/credential preservation checks BEFORE
   deleting private fixture state. Session currently reports
   `independent_command_replay_complete=False` and `release_certified=False`.
8. Commit/freeze the admitted source and staged artifact closure before a fresh
   installed local-Mac candidate. Preserve failed candidates and all owned
   process handles. Audit raw evidence before further source edits. Close .10
   only when its actual acceptance criteria pass; .9 recovery/migration and the
   full release aggregate remain separate unfinished requirements.

Good parallel split: one agent handles route/log evidence, another independent
Session/cleanup review and tests, another cross-Machine credential controls;
the main agent owns dispatcher and artifact/evidence integration. Assign files
explicitly to prevent overlapping edits.

## Reusable local artifacts (revalidate before use)

Registry inputs, no Docker execution certification:

- `.artifacts/registry-3.1.1-bounded-replay-1/layout`, also independently admitted
  `.artifacts/registry-3.1.1-input-candidate-2/layout`.
- `.artifacts/registry-3.1.1-load-candidate-1.tar`: 18,847,232 bytes, SHA-256
  `b971780e748c210e9b26ee86334a762160dedeccbada5beabf158d6b765cc9c3`.
- Pin: `config/docker-registry-artifact-v3.1.1.json`; selected manifest
  `sha256:bc68ba48dae0e0423bb885c8d07d20c3210febbe996d38d54d32c574fda690ae`.
- Registry binary SHA-256
  `669f0d9892da6ccd44a40954f39a3b929f4455d7ed02a806828346feac572834`,
  Go 1.25.9, Distribution v3.1.1. Bind the offline binary proof to route metadata;
  runtime logs must not self-certify these pins.

Earlier production/config milestone (not this checkpoint's release closure):

- Commit `17a0a442` owns Docker client configuration/credentials per Machine.
- Signed installed artifacts `.artifacts/topology-cli-installed-BVlJY5`, from
  that source commit. Later Python changes are not implicitly part of its digest.
- Guest bundles under
  `.artifacts/sandbox-vm-e2e/20260906T210423Z/retained-guest-bundles/`:
  `developer` and `container`; Engine 29.7.2, youki-only Linux OCI.
- Canonical host Docker `/usr/local/bin/docker` resolves to OrbStack's
  `/Applications/OrbStack.app/Contents/MacOS/xbin/docker-tools`, CLI 29.4.0.
  Never use its default daemon/context; every command targets the admitted
  Machine-private config/context. Stage Compose/buildx under their expected names.
- Physical evidence `.artifacts/managed-docker-config-startup-candidate-1` and
  `.artifacts/managed-docker-config-delete-candidate-1`: focused lifecycle/config
  ownership passes, not actual registry authentication. Startup disks are retained;
  Delete's exact owned stores were removed. Do not delete/reuse these candidates.

No outstanding root test process remains at handoff. Do not infer absence of all
host services from that statement; inspect live state before any resumed run.
Preserve unrelated Beads runtime files, `.claude/worktrees/`, `.impeccable/`,
`crates/.vz-runtime/`, `examples/rust-sandbox/`, `private-vz/`, and existing stashes.
Stage explicit task paths only. Sync issue metadata with `bd dolt push` before
pushing code; never use `bd sync` or force-push to resolve ordinary conflicts.
