# vz 0.4 CLI help snapshot fixture

`help-snapshot.txt` is the exact bytes bare `vz` (and `vz --help`) must print.
The topology lane (`scripts/helpers/developer_environment_e2e.py`, sub-check
`gate.cli.legacy_removal_and_bootstrap__bare_help`) compares the installed
release binary's stdout byte-for-byte against this file.

Provenance: created 2026-09-07 from the DEV release candidate
`.artifacts/vz-0.4-release-candidate-dev-1/bin/vz` (a `local-test-signed`
0.4.0-dev build, source commit 05cf2d66). It is byte-identical to the Rust
driver's snapshot `crates/vz-cli/tests/fixtures/dev-help.txt`. Its content
still carries the "Implementation status: DEV" trailer; the release-grade
snapshot must be reviewed and re-pinned before certification.
