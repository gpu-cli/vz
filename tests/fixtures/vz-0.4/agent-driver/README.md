# `tests/fixtures/vz-0.4/agent-driver`

The checked-in deterministic agent driver GOAL-0.4.0 criterion 12 requires, and
the fixed schedule it runs.

    12. **Agentic workload:** a checked-in deterministic agent driver runs at
        least three isolated workers against separate Environments and two
        cooperating workers against a Linux Machine and a native macOS Machine
        in one Environment using a fixed schedule, a declared cross-target
        service path, and synchronization barriers. Workspace writer policy is
        enforced and every output, cancellation, PTY, exit status, event, and
        receipt maps to the exact Environment, Machine, worker, and request.

| file | what it is |
|---|---|
| `schedule.json` | the fixed schedule: nine workers, seven rounds, every step's program and expected outcome |
| `driver.py` | runs that schedule against the installed `vz` and records what the runtime answered |

The gate sub-check that runs them and judges the result is
`gate.agent.deterministic_workers__deterministic_agent_workers`, in
`scripts/helpers/developer_environment_checks.py`.

## What makes it deterministic

* **The schedule is data.** `schedule.json` fixes the workers, the rounds, the
  steps in each round, each step's program (spelled per target OS), and each
  step's expected outcome. `driver.py` adds no step of its own and reorders
  nothing.
* **Rounds never overlap.** A round starts only after every step of the
  previous round has terminated, so the set of executions in flight at any
  moment is a property of the schedule alone.
* **Steps inside a round overlap on purpose.** Every non-held step in a round
  rendezvouses on one barrier before it spawns anything. `twin_crosstalk`,
  `twin_cancellation` and `writer_policy` are only meaningful concurrent, and
  the barrier is what makes them concurrent rather than usually-concurrent.
* **The cooperating barrier is the runtime's own event.** The round that needs
  a service to be up waits for that execution's `execution_ready` record before
  releasing the barrier, never for a sleep.
* **Identities are derived, not drawn.** A step's `--request-id`,
  `--idempotency-key` and guest token are all derived from
  `(run token, step id)`. Two runs of one schedule under one run token issue
  byte-identical command lines, so a failure names a step rather than a race.

## What it does not do

The driver asserts nothing and grades nothing. It records, per step, what it
*intended* (binding, Environment selector, Machine, request identity, argv,
cwd) and what the runtime *answered* (every `--json` record verbatim, the raw
terminal transcript of a PTY step, the process exit status, the wall-clock
span). Comparing answer against intent is the gate check's job, which is what
stops this fixture from certifying itself.

## Running it by hand

    python3 -B driver.py --schedule schedule.json --plan plan.json --transcript out.json

`plan.json` is written by the caller and binds each symbolic binding name in
the schedule to a real target:

```json
{
  "schema_version": 1,
  "kind": "vz-0.4-agent-plan",
  "run_token": "vzag-0123456789abcdef",
  "cli": "/path/to/release/bin/vz",
  "env": {"PATH": "/usr/bin:/bin"},
  "bindings": {
    "isolate-a": {
      "cwd": "/path/to/project",
      "environment": "default",
      "machine": "machine-0",
      "target_os": "linux",
      "env": {"VZ_RUNTIME_STATE_DB": "..."},
      "params": {"path": "/vz-storage/rw/agent.txt", "port": "8080"}
    }
  }
}
```

A binding the plan omits is recorded as skipped rather than faked, so a host
with no registered native macOS template produces a transcript that says the
cooperating pair never ran.
