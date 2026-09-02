# Developer Environments: the primary vz product

Status: canonical product reset plan

## Mission

vz makes reproducible, parallel, container-like Developer Environments on developer workstations. A Developer Environment is project-scoped, persistent when useful, disposable on demand, and runs its selected target operating system in native form. Host platform and target OS are separate dimensions of one product contract.

Linux is the universal target: Linux-on-macOS now, Linux-on-Linux next, and Linux-on-Windows after that. Native targets are additive: macOS-on-macOS now and Windows-on-Windows finally. The persisted identity, gRPC API, CLI, configuration, and core scenarios are host/target-neutral; each supported pair supplies native isolation, process, filesystem, networking, supervision, and capability adapters.

The locked-down sandbox/container capability remains valuable as an implementation substrate and advanced restricted mode, but it is no longer the product center. Users should not have to compose a VM, sandbox, workspace container, Docker daemon, socket proxy, and context themselves. Creating or first using a Developer Environment reconciles all of them as one lifecycle.

## Product contract

- `Developer Environment` is the primary user-facing object and API resource.
- Every environment has a stable identity derived from an explicit name or project/worktree identity.
- Every environment has an immutable target specification: OS, architecture, image/version, and requested capabilities. Unsupported host/target pairs fail explicitly and never substitute another target.
- Every environment owns its native isolation boundary, workspace, persistent state, processes/services, networks, credentials, endpoints, and capability-specific resources.
- Linux targets include private Docker by default. The host's unmodified Docker CLI, Compose, and buildx select that environment through a managed context. Docker may start lazily but requires no separate enablement.
- macOS targets run native macOS processes and services and never silently create a Linux companion to provide Docker.
- Windows targets run native Windows processes/services under a future Windows-native isolation capability; they do not inherit Linux OCI assumptions.
- Parallel environments fail closed and cannot observe or control one another's processes, containers, storage, networks, caches, endpoints, ports, or credentials.
- youki is the only OCI runtime shipped or executed in Linux targets. runc/crun fallback is forbidden. This invariant is inapplicable to native macOS and Windows targets.
- The existing locked-down `Container` kernel profile becomes the `Hardened` profile at the public UX boundary. Its security defaults are not weakened.

## Delivery phases and dependencies

```text
Product contract and terminology
            |
            v
Host/target identity, class, and lifecycle
      /        |          |              \
     v         v          v               v
CLI      Linux Docker  native macOS  shared isolation
     \         |          |              /
      \        v          |             /
       +-- Linux Engine Adapter          /
               |              |         /
               v              v        v
       Linux-on-Mac gate   macOS-on-Mac gate
                \             /
                 v           v
                   Mac GA
                     |
               Linux-on-Linux
                     |
              Linux-on-Windows
                     |
             Windows-on-Windows
```

- Phase 0 establishes canonical naming, profile boundaries, compatibility policy, and status language.
- Phase 1 makes host/target identity, class, capabilities, and lifecycle explicit. All runtime implementation phases depend on it.
- The CLI, Linux Docker, native macOS, and shared-isolation tracks can proceed in parallel once Phase 1 lands.
- Phase 5 depends on the CLI selection contract, Docker service, and isolation/storage primitives.
- Two immediate release-built Mac gates validate Linux-on-macOS and macOS-on-macOS, including simultaneous cross-target isolation.
- Phase 7 launches the Mac product only after both target lanes are green.
- Phase 8 implements Linux-on-Linux, Phase 9 Linux-on-Windows, and Phase 10 Windows-on-Windows.

## Success criteria

- From a project or worktree on the current Apple Silicon Mac, one command creates or reuses either a Linux or macOS Developer Environment through the same lifecycle and configuration shape.
- Linux targets reconcile Docker automatically; the CLI prints a stable managed context and Docker/Compose/buildx work without Docker Desktop's daemon. Docker commands on a macOS target return an actionable unsupported-capability error.
- macOS targets pass native process, filesystem, service, networking, persistence, and lifecycle gates from pinned images.
- At least two Linux environments operate concurrently with independent Docker engines and no state, socket, network, port, credential, or lifecycle cross-talk; Linux and macOS environments also pass cross-target isolation.
- Stop preserves environment identity and persistent state; restart restores it; delete removes it and only its managed Docker context.
- A dedicated Linux Docker gate passes twice on every Linux-target host. Target-native gates pass twice for macOS and later Windows targets.
- Evidence proves Docker/containerd/BuildKit executed pinned youki only in Linux targets; macOS and Windows evidence proves their native target contracts instead.
- Existing workspace gates and real local `vz` Linux VM regression suites pass.
- README, canonical docs, CLI help, examples, site, issue tracking, and release messaging all describe the same Developer Environment product and accurately distinguish shipped from planned behavior.

## Existing work to absorb

The new epic must re-scope or link the existing Docker and Developer-environment work rather than duplicate it: `vz-5in`, `vz-yr9`, `vz-k3v`, `vz-7ez`, `vz-0ml`, `vz-avq`, `vz-kna`, `vz-767`, `vz-219`, `vz-305`, `vz-0yt`, `vz-dko`, `vz-ehz`, `vz-wtz`, `vz-xym`, `vz-1jw`, `vz-bgd`, and the completed youki-only BuildKit work in `vz-356`.

## Plan files

- `architecture.md` — canonical object model and invariants
- `00-product-contract.md` — mission, language, profiles, compatibility policy
- `01-environment-lifecycle.md` — first-class class/identity/state contract
- `02-developer-cli.md` — converged project and named-environment UX
- `03-implicit-docker.md` — private Docker service and youki integration
- `03-native-macos.md` — macOS-on-macOS Developer Environment integration
- `04-isolation-storage-network.md` — persistence and parallel boundaries
- `05-host-docker-bridge.md` — per-environment proxy and Docker contexts
- `06-local-mac-validation.md` — extensive release-built Mac E2E contract
- `06-macos-target-validation.md` — native macOS target release gate
- `07-migration-launch.md` — macOS deprecations, docs, site, and launch
- `08-linux-parity.md` — Linux backend and identical scenario parity
- `09-windows-parity.md` — Linux-on-Windows backend and parity
- `10-windows-native.md` — Windows-on-Windows native backend and parity
