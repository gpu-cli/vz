---
name: plan-to-beads
description: Convert a planning document or folder into Beads issues with acceptance criteria and dependencies. Use when asked to import plans or create tracked work from them.
---

# Plan to Beads

Turn the requested plan into a small, actionable issue graph. Follow AGENTS.md
for Beads coordination and verification requirements. Creating issues does not
also authorize implementing them.

Read the overview and documents needed to understand the requested scope,
dependencies, and completion criteria. Inspect existing issues, including closed
ones, for overlapping outcomes and plan references before creating duplicates.
Similar titles alone do not establish that two issues represent the same work.

## Shape the work

- Group by independently reviewable outcomes. Use an epic or feature when it
  helps navigation; a small plan may need only a task. Do not mechanically turn
  every heading into an issue or impose session-duration estimates.
- Include the expected behavior, relevant plan paths/sections, acceptance
  criteria, and required verification. Add implementation hints only when they
  materially constrain the solution.
- Add dependencies for actual prerequisites. File numbering and containment do
  not establish blocking order; absence of a dependency note does not prove
  tasks are independent.
- Put verification in acceptance criteria by default. A separate integration or
  release-gate issue is useful when it spans multiple implementations. It does
  not waive an individual task's required backend evidence.
- Use dependencies at the level that controls the actionable work. Do not assume
  a dependency on a parent automatically blocks its children. Check the ready
  queue and add task-level edges when required.

## Create and inspect

Use the installed `bd <command> --help` for supported options. For example:

```bash
bd create --title "Outcome" --type task --body-file /path/to/issue.md
bd dep add <dependent-id> <prerequisite-id>
bd show <id>
bd ready
bd blocked
bd dolt push
```

Capture IDs as issues are created; use `--parent <id>` when grouping children.
Pass multiline descriptions through a body file or structured input rather than
interpolating plan text into shell commands.

Inspect the resulting hierarchy and readiness against the intended graph,
including cycles, missing prerequisites, and unintended blockers. Preserve
independence where supported by the plan; parallel execution is optional.
Summarize the created/reused issue IDs, what can start, and any unresolved scope.
