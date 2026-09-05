# Developer Environment definition (DEV)

This is the minimal **authoring** example for the 0.4 ProjectDefinition. It is
validated by the CLI's real definition loader and the published
[JSON schema](../../schemas/vz-project-definition-v1.schema.json). It is not a
release-certified runnable environment: the five-verb lifecycle, installed
authoring bundle, and aggregate local-Mac gate are still in development.

Copy `vz.json` into a project, replace the example `project_id` with a unique
opaque ID once (for example, `prj_` followed by a newly generated UUID without
hyphens), and commit it. Keep that ID across clones, moves, and worktrees; do not
reuse this example's ID for unrelated projects. No `vz init` command is needed.

Before execution, select an exact verified appliance from the target-qualified
artifact catalog and fill its `version`, `channel`, and `digest` into `target`.
The example deliberately does not invent a published 0.4 artifact digest or
silently use a mutable latest image. Structural validation alone does not
establish artifact availability, supported capabilities, or readiness.

The definition describes a reusable topology, not a single VM or worktree. Each
named Environment gets distinct Machine identities, storage, endpoints, and
lifecycle. Multiple Environments may bind to the same worktree. The Developer
Linux Machine implicitly owns its private Docker stack; no project-wide Docker
socket or extra Docker-enable field belongs in this file. Native macOS and
Hardened Linux Machines do not acquire Docker.

The intended public workflow is `vz up`, `vz exec`, `vz status`, `vz stop`, and
`vz delete`, with explicit Environment/Machine selectors where needed. Typed
APIs handle richer operations. Existing installed 0.3 binaries do not implement
this complete workflow.

Validate structure with a Draft 2020-12 JSON Schema validator, then use the
typed `ProjectDefinition::validate()` contract for unique names, reference
integrity, and profile rules. Runtime admission additionally checks the actual
host×target pair, pinned artifacts, resource limits, and ownership. The schema
does not authorize networking, host imports, storage mounts, or capabilities.
