# Container I/O fixture (DEV)

This sibling fixture supplies public deterministic guest workloads, not Docker
compatibility evidence. The host harness must bind the exact Python ARM64 image,
fixture bytes, owned Machine context/container and all input/output records.
The image contains only public code, a root marker and a deliberately non-executable
file. `/fixture` and `/workspace` are `0755`; public files are `0644`.

Run `python3 /fixture/probe.py MODE TOKEN`, with `TOKEN` matching
`vzio-[0-9a-f]{24}`. JSON records are canonical UTF-8 with sorted keys, compact
separators and exactly one newline; every record has `schema_version:1`, `type`
and `token`. Invalid input/timeout emits only `VZ_CONTAINER_IO_CONTRACT_REJECTED`
on stderr and exits 70. No network or downloaded runtime inputs are used.

- `stream`: at most 1 MiB binary stdin, actual EOF, then a fixed 100 ms delay.
  stdout is `vzio|TOKEN|stdout-begin\n`, the unchanged input, a newline and
  `vzio|TOKEN|stdout-end\n`. stderr contains the corresponding `stderr-begin`
  and `stderr-end` lines. Exit 37. The standard input is `bytes(range(256))*257`.
- `tty`: requires all three descriptors to be terminals, ICANON and ISIG already
  enabled; disables only ECHO and restores guest termios in `finally`.
  `tty_ready` has `isatty:[true,true,true],rows,cols`; `size\n` emits
  `tty_size` with rows/cols; `exit\n` emits `tty_done` with exit_code 37.
  At most four complete commands are admitted within the fixed 30-second window.
  Actual SIGINT emits `observed_signal` with signal `SIGINT`, exit_code 130.
  Terminal ONLCR translation belongs to the host PTY recorder, not this JSON
  encoder. The host must separately prove its own terminal restoration.
- `service`: creates/verifies its exact token-owned health file and stays alive
  for at most 300 seconds. `service_ready` on each stream includes pid,
  health `starting`, and output `stdout`/`stderr`. SIGUSR1/SIGUSR2 atomically update
  healthy/unhealthy state and emit `health_changed` with healthy boolean and
  signal. SIGTERM emits `observed_signal` and exits 143. Host KILL must actually
  kill the process; there is no fake KILL handler.
- `health`: exact healthy state exits 0; exact starting/unhealthy state exits 1; missing,
  malformed, redirected or foreign state is an operational error, not unhealthy.
  The canonical health file has `schema_version,type,token,state`; `state` is
  exactly `starting`, `healthy` or `unhealthy`. The host must configure a 30-second
  health start period and observe Engine's actual starting/healthy/unhealthy
  transitions; the file itself is not proof of Engine health scheduling.
- `exec`: `exec_identity` reports uid/gid/cwd, pid/pid1, fixed root marker,
  `namespaces` and `pid1_namespaces`, followed by exact `exec-stdout`/`exec-stderr`
  token marker lines and exit 37. Namespace IDs use kernel readlink syntax.
  Nonroot inability to read root PID1 namespace links is typed permission-denied
  evidence, never equality proof. Compare self IDs with a separate admitted root
  exec to establish the nonroot process's namespace membership.
- `exit CODE`: only literal 0,37,130,137,143 are accepted, with empty streams.
  A numeric exit mode cannot prove signal delivery; the host must use real
  SIGINT/SIGTERM/KILL, exact target identity and independent lifecycle evidence.

Offline tests use finite local Python subprocesses and owned PTYs only. They do
not establish Docker stream demultiplexing, attach half-close, resize forwarding,
container restart/wait/event behavior, health scheduling or Machine isolation.
