---
name: tmux-cli-test
description: Test interactive CLI and TUI behavior in a terminal using tmux. Use for keyboard navigation, prompts, redraws, or terminal-dependent behavior; ordinary command output and exit-code checks can run directly.
---

# Test terminal interactions

Use a tmux session to exercise the requested behavior and capture evidence.
Choose scenarios for the change: readiness, input, navigation, cancellation,
resize, or error recovery as relevant. Use disposable data for actions that
modify state.

## Helpers

The optional [Bash helpers](scripts/tmux_helpers.sh) provide bounded condition
waits, input, assertions, and captures. They require `tmux` and `bc`, and sourcing
them enables `set -euo pipefail`. Source them in a dedicated Bash invocation.

`tmux_start` kills a same-named session first, so choose a unique session name and
clean up only sessions created for this test. For example, from the repo root:

```bash
source .claude/skills/tmux-cli-test/scripts/tmux_helpers.sh
session="vz-cli-test-$$"
trap 'tmux_kill "$session"' EXIT
tmux_start "$session" "env PS1='cli-test> ' bash --noprofile --norc -i"
tmux_wait_for "$session" "cli-test>" 10
tmux_type "$session" "printf '%s%s\\n' terminal- ready"
tmux_send "$session" Enter
tmux_wait_for "$session" "terminal-ready" 10
tmux_capture "$session"
```

Replace the shell and expected text with the application and observable state
being tested. Prefer a bounded wait for an expected state over a fixed delay.
A timeout should retain a frame/log for diagnosis.

| Helper | Use |
| --- | --- |
| `tmux_send <session> <keys...>` | Special keys such as Enter, Escape, C-c |
| `tmux_type <session> <text>` | Literal input |
| `tmux_wait_for <session> <text> [seconds]` | Wait for visible text |
| `tmux_wait_for_regex <session> <regex> [seconds]` | Wait for an extended regex |
| `tmux_wait_gone <session> <text> [seconds]` | Wait for text to disappear |
| `tmux_capture <session>` / `tmux_capture_ansi <session>` | Capture plain/ANSI text |
| `tmux_assert_contains <session> <text>` | Check current pane text |

A visible input echo does not prove an action succeeded. Assert the resulting
application state. Session disappearance alone does not prove a zero exit code;
use process/log evidence when exit status matters.

For an already selected Docker test environment, the optional
[Docker helpers](scripts/tmux_docker_helpers.sh) target an existing session via
`TMUX_DOCKER_CONTAINER` and `TMUX_DOCKER_SESSION`. They use the active Docker
connection; confirm that it is the intended Machine/container. Do not substitute
a global daemon for a vz Machine's private Docker.

Report the command, interactions, observed result, and evidence. Text captures
support content assertions; color/layout claims require inspecting the actual
rendering. Broader backend certification still follows AGENTS.md.
