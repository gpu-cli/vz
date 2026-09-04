---
name: implement-bd
description: Implement Beads issues and record verification and remaining work. Use when asked to work on a tracked issue or pick up ready Beads work.
disable-model-invocation: false
user-invocable: true
argument-hint: "[issue-id]"
---

# Implement Beads work

Deliver the requested issue's outcome and keep Beads useful for the next session.
Follow the repository's AGENTS.md for product constraints, verification, and
delivery. Adapt the amount of planning and investigation to the task.

## Understand and implement

Read `bd show <id>`, relevant children/dependencies, and referenced acceptance
criteria. If no issue was supplied, use `bd ready` to find work within the user's
requested scope. Claim the selected issue with
`bd update <id> --status in_progress`.

Inspect the relevant code and contracts, then implement and verify the behavior.
For an epic or feature, work through actionable children and check the aggregate
acceptance criteria before closing the parent. Shared contracts may require
changes and checks in consuming crates.

A small task can be done directly. A written plan or delegation is useful only
when complexity and available tooling justify it; neither is a required phase.
Do not depend on named agent types or tools that the session does not provide.

## Verify and track

Use checks that can detect regressions in the changed behavior. Runtime/backend
work requires the end-to-end evidence specified in AGENTS.md; passing unit tests
or a successful agent report is insufficient.

Investigate failures using logs and, when useful, a baseline. A failure in an
unchanged crate is not automatically pre-existing. Fix regressions within the
task's scope; record unrelated problems without turning the task into a cleanup.
Continue debugging while there is a useful next step. Ask for input when a
missing decision, access, or external dependency actually blocks progress.

Keep durable notes on meaningful progress, evidence paths, and unresolved work.
Check for an existing issue before filing a concrete follow-up. Do not reset
another worker's in-progress issue just because it is still open.

Close with `bd close <id> --reason "..."` only when acceptance criteria and
applicable gates pass. Otherwise leave it open with the remaining requirement.
Push issue data with `bd dolt push` and deliver code according to AGENTS.md.
Report the outcome, verification, and any blocker without a fixed report template.
