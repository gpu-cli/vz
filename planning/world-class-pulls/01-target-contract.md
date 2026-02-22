# 01 - Target Contract

## Objective

Define a concrete pull contract that must hold for both CLI and SDK consumers.

## Functional Contract

Required behavior:

- Resolve and pull `registry/repo:tag` and `@digest` references.
- Resolve image index to `linux/arm64` variant deterministically.
- Download missing blobs in parallel with bounded concurrency.
- Verify blob digest and expected media type before commit.
- Unpack layers and assemble rootfs deterministically with whiteout correctness.
- Persist image cache atomically with no partial-commit corruption.

## Performance Contract

Baseline source:

- Current implementation in `crates/vz-oci/src/image.rs` and `crates/vz-oci/src/store.rs`.

Targets:

- `T1`: cold pull of representative service image (`nginx`, `python`, `node`) is `>= 2x` faster than current baseline on identical hardware/network.
- `T2`: cold pull of common 6-image cohort is `>= 3x` faster in total wall-clock time.
- `T3`: warm pull (all blobs present) returns without network blob fetches and with p95 under 1 second.

## Reliability Contract

Targets:

- Pull state survives host-process interruption with resumable recovery where registry supports HTTP range.
- Retry policy handles transient errors (`timeout`, `connection reset`, `429`, `5xx`) with bounded exponential backoff.
- Pull never reports success unless manifest/config/layer writes are fully committed.

## Correctness Contract

Mandatory checks:

- Descriptor digest verification for every blob.
- Manifest/config media type sanity checks.
- Layer extraction safety checks (path traversal, invalid whiteouts).
- Deterministic rootfs output for identical image digest + layer set.

Hard-fail rules:

- Any digest mismatch.
- Any unsupported/invalid digest algorithm.
- Any malformed manifest/config/layer content.

## UX Contract

Required status visibility:

- Pull phase transitions: `resolve`, `auth`, `manifest`, `config`, `layer_download`, `layer_unpack`, `assemble`, `commit`.
- Per-layer progress events with bytes transferred, retries, elapsed.
- Stable error envelope with machine-readable error kind and user-focused message.

UI event quality requirements:

- Every event carries `pull_id`, monotonic `seq`, timestamp, and `phase`.
- Event ordering is deterministic per `pull_id`.
- Progress never silently regresses:
  explicit rollback/retry events are required when percent/ETA moves backwards.
- Steady cadence while active:
  no silent gaps longer than 2 seconds without heartbeat/progress updates.
- Throughput and ETA are smoothed for readability (no frame-to-frame jitter spikes).
- Final terminal event is guaranteed: `completed` or `failed` with summary counters.

UI fields required per pull:

- Overall percent.
- Downloaded bytes and total bytes when known.
- Active layer count and completed layer count.
- Instant throughput and smoothed throughput.
- ETA (nullable when unknown) plus reason if unknown.

## Compatibility Contract

Registry and transport:

- Docker Hub, GHCR, ECR Public, and private OCI-compliant registries.
- Auth modes: anonymous, Docker config helper, explicit basic credentials.
- Mirror fallback policy is deterministic and observable.

## Non-Goals (v1)

- P2P layer exchange.
- Content trust policy enforcement beyond digest verification (signature verification is planned as opt-in later).
- Lazy filesystem mount technologies (`stargz`/`nydus`) in first milestone.
