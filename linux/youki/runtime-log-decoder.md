# Actual youki runtime-log decoder compatibility

This stdlib-only Go probe replays the real log emitted by the source-built
youki binary. Unit tests alone are not compatibility evidence. It does not boot
a VM, run a fallback runtime, establish arbitrary input-directory provenance,
or certify Docker parity. The parent authenticates the pinned build and retains
its complete candidate manifest.

## Exact source contract

The installed containerd startup log identifies **2.3.3**, commit
`aad11006b869517fcd3009450b6f82da282e1a9b`. Its
[getLastRuntimeError implementation](https://github.com/containerd/containerd/blob/aad11006b869517fcd3009450b6f82da282e1a9b/cmd/containerd-shim-runc-v2/process/utils.go)
has source-file SHA-256
`95c47a9c3e3520fb4110a1793ce12c9ddcf1506fe7c9787ae040527a94932567`.
The Go struct has string `Level`/`Msg` and `time.Time` `Time`. Only lowercase
`error` is selected; the last matching `Msg`, trimmed, wins. The probe preserves
that decoder behavior, then separately enforces this fixture's exact record.
The short reproduced decoder loop is Apache-2.0, Copyright The containerd Authors.

Pinned youki **0.7.0**, commit `94ba653efbb180ce04650f6ae01a8e6bc8f96d92`,
[uses tracing's JSON formatter](https://github.com/youki-dev/youki/blob/94ba653efbb180ce04650f6ae01a8e6bc8f96d92/crates/youki/src/observability.rs).
Its uppercase level and `message`/`timestamp` fields do not satisfy the decoder's
lowercase error and `msg` contract. Valid JSON alone is insufficient. The local
runtime-log patch must produce `level`, `msg`, `time` without changing the
underlying error. `inputs.env` pins the source archive and local patches.

## Required actual-runtime generation

The pinned Rust/Alpine Docker build, after copying its freshly built binary to
`/result/youki`, creates a private `/inputs/runtime-log-root` and executes once:

```sh
/result/youki --root /inputs/runtime-log-root \
  --log /result/runtime-log.json --log-format json \
  create --bundle /inputs .
```

Capture stdout/stderr verbatim in `/result/runtime-log-stdout.txt` and
`/result/runtime-log-stderr.txt`; record the actual exit code in
`/result/runtime-log-exit-status.txt`. It must equal `1\n`, not a signal/timeout
or substituted status. The invalid ID `.` is rejected by `validate_id` before
OCI spec parsing or namespace/container creation. Retain all four files with
the actual `youki` binary and `inputs.env` in the candidate manifest.

The sole error record must decode to the entire exact message:

```text
error in executing command: container id can't be used to represent a file name (such as . or ..)
```

Stdout must be empty. Stderr must be `Error: ` followed by the same underlying
invalid-ID error and a newline. The JSON record needs a nonzero RFC3339 timestamp,
lowercase error level, no duplicate/unknown fields, and no stale file tail.
This unspanned main-program event has exactly five top-level keys: `level`,
`msg`, `time`, `target` (exactly `youki`), and `fields` (exactly one `message`
equal to `msg`). The retained nested event must not override authoritative
metadata. Other runtime events may carry structured fields/spans; this probe
deliberately authenticates only the deterministic invalid-ID event.

## Host replay, without adding Go to the builder

Use the already installed Go SDK; retain `go version` beside the replay output.
The inspected host SDK is `go1.25.4 darwin/arm64`. No module downloads are needed.

```sh
GO111MODULE=off GOTOOLCHAIN=local go test -v linux/youki/runtime-log-decoder.go \
  linux/youki/runtime-log-decoder_test.go
GO111MODULE=off GOTOOLCHAIN=local go run linux/youki/runtime-log-decoder.go \
  --candidate /absolute/path/to/retained-youki-candidate
```

The second command is mandatory for candidate verification: it reads the actual
build outputs and prints an exact-error proof binding the binary, raw log,
stdout, stderr, exit status and source input hashes to the pinned containerd
decoder source. Reject nonzero exit, missing files, changed pins or wrong error;
do not repair logs or claim the synthetic unit tests replace this command.

The unit suite has no conditional or skipped actual-runtime test. Actual output
verification is the separate mandatory second command, not an optional test.
