---
name: tui-review
description: Review terminal UI usability and visual quality, including navigation, input handling, feedback, and resize behavior. Use for a TUI UX review or audit, rather than routine CLI correctness checks.
---

# Review terminal UX

Evaluate the requested user flows in the running application when available.
If only source or captures are available, explain that limit and avoid claiming
live interaction or responsiveness was tested.

Use the application's own help and intended workflows to select interactions.
The [heuristics reference](references/heuristics.md) offers prompts for deeper
reviews; load relevant sections rather than treating every item as a test gate.

## Gather evidence

Use an isolated tmux session or another available terminal harness. The sibling
[tmux-cli-test skill](../tmux-cli-test/SKILL.md) provides optional helpers.
Choose representative states and terminal sizes for the application, including
a constrained size when layout is in scope. Wait for meaningful state changes
with bounded timeouts.

Capture pane text for content and ANSI output or screenshots for visual claims.
Inspect images you rely on. If a renderer such as `freeze` is available,
`tmux capture-pane -t <session> -e -p` can supply ANSI input; verify that rendering
preserves the colors before judging them. Use a process timeout for a renderer
that may hang, and report failed captures rather than treating them as evidence.

If terminal configuration needs adjustment, limit it to a dedicated test server;
do not change the user's global tmux settings. Clean up only test resources.

## Judge the experience

Prioritize blocked workflows, confusing state, lost input, and inaccessible
controls. Assess hierarchy, readability, and consistency in context. Borders,
monochrome themes, and ASCII symbols are valid choices; color counts and imitation
of another product are not quality gates. Use timing claims only when measured.

Present actionable findings ordered by user impact, with reproduction steps,
observed behavior, expected behavior, and evidence or code locations where known.
Separate verified defects from design suggestions and untested areas. Grades,
comparison products, and exhaustive screenshots are optional, driven by the
request. A review does not imply permission to redesign the application.
