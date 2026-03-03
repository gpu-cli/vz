# e2b vs `vz`: capability gap analysis

Date: **2026-02-26**  
Scope: public `e2b` platform features vs local `vz` capabilities.

---

## Executive summary

`vz` is a **local** cross-platform runtime for OCI containers, compose stacks, and macOS VM automation.  
`e2b` is a **managed sandbox platform** with API-first lifecycle control, templates, tenancy/auth, and rich observability/access integrations.

So most of the difference is **product model**, not just feature flags:
- `vz` = runtime on your machine
- `e2b` = hosted platform for creating/managing many sandboxes as a service

---

## Big-picture architecture comparison

```
+----------------------+            +----------------------------+
|         e2b          |            |             vz             |
|----------------------|            |----------------------------|
| REST/SDK control     |            | Local CLI (`vz`)            |
| Auth + tenant model  +----------->| Optional API server (local) |
| Remote sandboxes      |            | Local runtime state          |
| Template service      |            | Compose / OCI / macOS VM      |
| Templates + registry  |            | local registry + caches       |
| Observability hooks   |            | Logs/events in CLI/store      |
| File/PTY/SSH APIs     |            | Exec/attach via local path     |
+----------------------+            +----------------------------+
```

```
                      e2b feature surface
                               |
                +--------------+--------------+
                |                             |
+---------------------------+     +---------------------------+
|       Developer API        |     |         Runtime layer      |
|  auth, builds, templates, |     |  execute containers/VMs,   |
|  files, webhooks,        |     |  checkpoints, logs         |
|  web endpoints            |     +-------------+-------------+
+---------------------------+                   |
                                                |
                        +-----------------------+----------------------+
                        |            `vz` local model                 |
                        |  One machine, one CLI operator, one user context|
                        +----------------------------------------------+
```

---

## What each project already does well

`vz` already has strong local runtime coverage:
- container lifecycle (`pull`, `run`, `create`, `exec`, `ps`, `logs`, etc.)
- compose stack orchestration (`stack up/down`, service exec, logs)
- macOS VM flows (`vm init/run/exec/save/restore/provision`)
- snapshot/restore at VM image artifact level (including patch/delta flows)

`e2b` already has strong platform/service-layer coverage:
- tenancy, auth, and API-first multi-sandbox orchestration
- template + cache + version/tag model
- remote sandbox file APIs (write/read/watch/upload/download)
- external integrations (PTY/SSH/tunnels/custom domain)
- event/webhook-driven operations and richer product telemetry

---

## Gap matrix (by capability)

| Area | e2b capability | `vz` equivalent today | Gap verdict | Effort |
|---|---|---|---|---|
| Auth & tenancy | API keys, account/team auth, CLI+SDK tokens | Local runtime/user context only | Missing | High |
| Sandbox API | Create/List/Get/Terminate/Metadata lifecycle endpoints | Local sandbox attach/list/terminate only, no product lifecycle API | Missing | High |
| Runtime modes | Pause/Resume with persistence expectations | No explicit managed pause/resume product flow | Missing | High |
| Persistence model | Snapshot persistence and resume product semantics | VM save/restore exists, but not unified remote sandbox snapshot model | Partial | Medium |
| Templates | Build definitions + cache + private registry + tags/versioning | No template-as-service layer | Missing | High |
| Filesystem APIs | Read/write/metadata/watch/upload/download via sandbox APIs | No dedicated sandbox file API surface | Missing | High |
| Connectivity | PTY, SSH, tunneling, custom domains, internet policy controls | Local attach/exec only | Missing | High |
| Observability | Lifecycle events API + webhooks + metrics + audit-like signals | Local logs/events and store inspection | Missing | High |
| Operational controls | Quotas/rate limits/plan model | No service-level quotas in CLI runtime layer | Missing | Medium |
| Collaboration | Git operations integrated to sandbox workflows | Not product-level in CLI | Missing | Medium |
| Integrations | MCP gateway/tooling bridge | Not present | Very high |
| Private enterprise | BYOC / enterprise deployment model | Not defined in current `vz` scope | Very high |

Legend:
- **Missing**: no direct equivalent in current `vz` docs/CLI.
- **Partial**: similar behavior exists but not the same product contract.
- **Very high**: requires new control-plane and service architecture.

---

## Priority path to close the highest-value gaps

1. **Foundation layer (high value / lowest disruption first)**
   - Add/standardize an external API contract for sandbox entities in `vz-api`.
   - Add auth + auth context into the control plane (token, tenant, role, quota context).
   - Expose lifecycle events to existing storage + event pipeline.

2. **Core platform parity**
   - Template + image/build manifest metadata model.
   - Snapshot + resume semantics as first-class API objects with policy.
   - Harden filesystem operations contract (read/write/watch operations).

3. **Connectivity and integrations**
   - PTY/SSH/connectivity endpoints and URL routing patterns.
   - Webhooks + callback delivery with retries/signing.
   - Public SDK bindings for stable API objects.

4. **Advanced differentiation**
   - MCP gateway integration
   - BYOC and enterprise controls
   - Private registry policy and rate-limiting as business controls

---

## Recommendation for planning

If the goal is **closest possible parity**, treat this as a full **control-plane rewrite** around sandbox entities, not just CLI features.

If the goal is **pragmatic incremental parity** for your team, implement in this order:
- local `e2b`-compatible API endpoints in `vz-api`
- sandbox lifecycle entity model
- template + storage abstraction
- filesystem + eventing
- connectivity primitives (PTY/SSH/proxy)
- SDK + auth plane

---

## Quick decision aid

If you only want a local-first `vz`, ignore:
- team auth + rate limiting
- webhooks/events-as-a-product
- MCP and BYOC

If you want an e2b-like service, start with:
- control plane (API/auth/templates)
- sandbox resource model
- file/session access APIs

Then backfill runtime internals without changing existing container/VM execution quality.

