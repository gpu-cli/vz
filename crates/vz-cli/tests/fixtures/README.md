# CLI retirement fixtures (DEV)

`dev-help.txt` is the exact current static help snapshot. It is deliberately
labelled DEV and is not the release goal's completed five-verb help snapshot.
Update it from the actual built CLI when a real lifecycle command lands; do not
add parser or dispatch stubs to satisfy a command count.

`config/cli-removal-v0.4.json` is the machine-readable removal inventory.
Its 160 development-baseline help paths were read recursively with `--help`
from the signed `.artifacts/topology-cli-installed-r93vAW/vz` binary, whose
SHA-256 is recorded. The hidden `debug` root and `vm linux e2e` were explicitly
included. The already-retired `stack` paths are recorded separately from the
normative migration inventory, not falsely described as observations from that
binary. No workload command was executed to obtain this inventory.

The integration tests invoke every recorded path directly, with `--help`, with
unknown nested arguments, and through `help <path>`. They require the stable
nonzero structured migration error, empty stdout, bounded completion, and
unchanged isolated project/runtime state. Another test places a real local Unix
listener and malformed state database at the selected runtime paths and proves
retired roots never connect or modify them. Parser unit tests bypass the
rejection preflight to prove no retired or hidden parser remains.

The immutable **released v0.3.20 binary has not been acquired and verified**.
Its required recursive release-baseline traversal is still pending. The current
source version string or an ad-hoc-signed development build does not replace
that release fixture. Fresh installed, signed checks and the complete physical
five-verb lifecycle gate remain required after changes to the CLI.
