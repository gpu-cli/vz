# 05 — Manual Linux Stack Harness

## Goal

Provide a repeatable host-side runbook for validating `vz stack` on Linux-native hosts across the compose dimensions that are most likely to fail in real environments.

## Scope and assumptions

- Focus: compose-compatible subset already implemented by `vz-stack`.
- Host assumption: Linux with `VZ_BACKEND=linux-native` selected, root or `CAP_NET_ADMIN` available.
- Runtime assumption: `youki` or `runc` available in `PATH`, common networking tools (`ip`, `iptables`, `iptables-nft` as available).
- Data directory assumption: `XDG_DATA_HOME` writable and enough disk for layer/cache storage.

## Automated config matrix (recommended first pass)

Use this lightweight parser-only harness to reach 50+ compose variants before manual runtime smoke tests:

- `vz stack config` parser coverage command:
- `crates` dir: `./crates/target/debug/vz` after `cargo build -p vz-cli`
  - `planning/linux-native-support/run-linux-stack-config-matrix.sh`
- Expected output: 60 pass variants + 10 expected failures across parser-edge conditions.
- Use `VZ_BACKEND=linux-native` only where command behavior is backend-aware.
- Use results here to identify parse regressions before executing `stack up`/`down` on a real host.

## Preflight checklist

- Verify backend: `VZ_BACKEND=linux-native vz --version`.
- Verify runtime probe: run the host capability check path in code (`vz-linux-native` probe CLI if/when exposed) or run `vz oci doctor` once available.
- Validate container runtime: `which youki || which runc`.
- Ensure cleanup baseline is clean:
  - `ip link show type bridge | grep vz-` should return none.
  - `ip netns list | grep vz-` should return none.
- Use an isolated working directory for each run.

## Test matrix (high impact)

1. **Compose parsing + scheduling shape**

- `single_service_basic`: one service, command keeps process alive (`sleep 300`).
  - Expect: stack id appears in `vz stack ls`, `vz stack ps` shows one service in running state.
- `two_service_linear_dep`: A depends on B (`depends_on`).
  - Expect: B starts before A; no flake in action ordering.
- `three_service_fan_in`: web + worker + db with parallelizable dependencies.
  - Expect: deterministic action ordering and no duplicate container creation on rerun.

2. **Image and command dimensions**

- `alpine_sleep`: `alpine:latest` with explicit shell command.
  - Expect: pull/create/start/ps and clean down path all succeed.
- `python_server`: `python:3-alpine` with simple `python -m http.server` and health check endpoint.
  - Expect: service stays ready while port forwarding works.
- `distroless_like_image`: minimal image without shell.
  - Expect: command is honored and service exits with explicit error if command invalid.

3. **Networking and service discovery**

- `bridge_ports_single`: service publishes one port, host mapping accessible.
  - Expect: HTTP probe from host succeeds while service runs.
- `bridge_ports_contention`: attempt to reuse mapped host port after first stack down.
  - Expect: first stack uses the port, second start fails fast with clear port conflict.
- `dns_and_healthy`: service uses hostname of sibling by compose service name.
  - Expect DNS resolution to peer service name succeeds.

4. **Lifecycle and control-plane API**

- `logs_and_exec`: start stack with two services and run:
  - `vz stack logs <stack>` for stream + bounded output.
  - `vz stack exec <stack> <service> <cmd>` to validate exec path.
  - Expect: both succeed and return exit code 0.
- `restart_and_scale`: stop then start same stack twice.
  - Expect idempotent behavior with no stale state collisions.
- `events_observation`: tail stack events for startup/shutdown transitions.
  - Expect event sequence for create/start/ready/shutdown in order.

5. **Failure resilience**

- `bad_image`: unsupported image reference.
  - Expect clear failure status and clean stack state.
- `invalid_dependency`: missing dependency name.
  - Expect deterministic failure and no orphan containers.
- `process_exit`: one service exits non-zero quickly.
  - Expect state and logs reflect failure; manual down removes artifacts.

## Minimal command flow

Use a repeatable sequence per case:

1. `VZ_BACKEND=linux-native vz stack up -f <compose.yml> --name <stack>`
2. wait for health/readiness signals and verify expected behavior.
3. `VZ_BACKEND=linux-native vz stack ps <stack>`
4. `VZ_BACKEND=linux-native vz stack events --stream <stack>` (or non-stream if supported)
5. run targeted `logs` / `exec` checks.
6. `VZ_BACKEND=linux-native vz stack down <stack>`
7. `VZ_BACKEND=linux-native vz stack ls` and host network cleanup checks.

## Suggested fixture templates

Keep files in a temporary directory and delete the folder after each scenario.

### `single_service_basic.yaml`

```yaml
services:
  api:
    image: alpine:latest
    command: ["sleep", "300"]
```

### `two_service_linear_dep.yaml`

```yaml
services:
  db:
    image: redis:7-alpine
    command: ["redis-server", "--save", "", "--appendonly", "no"]
  web:
    image: nginx:1.25-alpine
    depends_on:
      - db
    ports:
      - "18080:80"
    command: ["nginx", "-g", "daemon off;"]
```

### `python_server.yaml`

```yaml
services:
  app:
    image: python:3.12-alpine
    command: ["python", "-m", "http.server", "8000"]
    ports:
      - "18081:8000"
```

## Pass/Fail reporting template

- Record each scenario as `PASS`, `FAIL`, or `BLOCKED` with:
  - `vz` command outputs,
  - service logs sample,
  - observed host network state,
  - time to first ready signal.
- Add short notes when failures are environment-specific (kernel/runtime/version).

## Exit criteria for this pass

- At least one happy-path run for each dimension above on a representative Linux distro.
- No manual intervention required beyond documented cleanup steps.
- Remaining blockers are converted into concrete follow-up issues with exact repro details.
