# TUI UX Heuristics Reference

Detailed pass/fail criteria for each review dimension, derived from Claude Code, OpenCode, and Codex — the three best-in-class AI terminal UIs.

## Table of Contents

1. [Responsiveness](#1-responsiveness)
2. [Input Mode Integrity](#2-input-mode-integrity)
3. [Visual Feedback & State Communication](#3-visual-feedback--state-communication)
4. [Navigation & Escape Hygiene](#4-navigation--escape-hygiene)
5. [Feedback Loops & Submission Flow](#5-feedback-loops--submission-flow)
6. [Error, Empty & Edge States](#6-error-empty--edge-states)
7. [Layout & Resize Behavior](#7-layout--resize-behavior)
8. [Keyboard Design & Discoverability](#8-keyboard-design--discoverability)
9. [Permission & Confirmation Flows](#9-permission--confirmation-flows)
10. [Visual Design & Color](#10-visual-design--color)

---

## 1. Responsiveness

**Principle**: Every keypress must produce visible change within one frame (~16ms render, <100ms perceived).

### What Best-in-Class Does

- **Claude Code**: Typing `!` instantly changes the prompt prefix from `>` to `!`. Typing `/` opens the command dropdown in the same frame. No "dead" keys.
- **OpenCode**: Typing `/` renders the command list inside the input border immediately. Token counts update live during streaming.
- **Codex**: Ghost text placeholder disappears the instant typing begins. Elapsed time counter (`11s`) ticks live during operations.

### Test Procedure

```bash
# For each key in the set: j, k, Up, Down, Tab, Enter, Escape, /, ?, !
BEFORE=$(tmux_capture "$S")
tmux_send "$S" "$KEY"
# Poll at 100ms — frame must differ
AFTER=""
for i in $(seq 1 5); do
    sleep 0.1
    AFTER=$(tmux_capture "$S")
    [ "$BEFORE" != "$AFTER" ] && break
done
[ "$BEFORE" != "$AFTER" ] || echo "FAIL: dead key '$KEY'"
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Navigation key (j/k/arrows) | Selection/cursor moves within 100ms | No visible change within 500ms |
| Mode trigger (/, !, @) | UI element appears/changes within 100ms | Delay >200ms or no change |
| Enter on input with text | Input clears AND response begins within 200ms | Input remains or no feedback for >500ms |
| Escape during processing | Working indicator stops within 200ms | Operation continues for >1s |
| Streaming text | Characters appear progressively, not in chunks | Text appears in large blocks with pauses |

### Anti-Patterns

- Rendering entire response before displaying (batch instead of stream)
- Key events queuing silently during rendering (input feels swallowed)
- Focus trap where a key produces no visible change and no error

---

## 2. Input Mode Integrity

**Principle**: Trigger characters must never hijack text input. A user typing prose that contains `/`, `@`, `!` must not accidentally open menus.

### What Best-in-Class Does

| Character | When it triggers | When it types literally |
|-----------|-----------------|------------------------|
| `/` | **Claude Code**: Any position (aggressive). **OpenCode/Codex**: Start of input only | Mid-word: `foo/bar` must NOT open menu |
| `!` | First character on empty input only (all three TUIs) | After any text: `hello!` types literally |
| `@` | **Claude Code**: Any position opens file picker. **OpenCode**: Any position for agent mention. **Codex**: Any position for file path | N/A — these TUIs accept it activating anywhere |
| `?` | **Claude Code**: Only when prompt is completely empty | After text: `what?` types literally |
| `&` | **Claude Code only**: First character on empty input | After text: `foo & bar` types literally |

### Test Procedure

```bash
# Test: mid-text slash must not trigger command menu
tmux_type "$S" "search for foo"
tmux_type "$S" "/"
tmux_type "$S" "bar"
tmux_assert_not_contains "$S" "commands" "mid-text / does not open menu"
# Verify the text typed correctly
tmux_assert_contains "$S" "foo/bar" "literal / in text"

# Test: ! after text must not enter bash mode
tmux_type "$S" "hello!"
# Prompt should still show normal prefix, not bash mode prefix
tmux_assert_not_contains "$S" "bash mode" "trailing ! is literal"

# Test: ? after text must not show help
tmux_type "$S" "what?"
tmux_assert_not_contains "$S" "shortcuts" "trailing ? is literal"
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| `/` mid-sentence | Types literally | Opens command menu |
| `!` after text | Types literally | Switches to bash mode |
| `?` after text | Types literally | Opens help overlay |
| `&` after text | Types literally | Switches to background mode |
| `@` mid-word | Context-dependent: ok to trigger file picker OR type literally | Crashes or produces garbage |

### Common Bugs

- Checking only the latest character instead of cursor position
- Not distinguishing "first character on empty input" from "first character on current line"
- Modifier keys (Ctrl+/) triggering the same as bare `/`

---

## 3. Visual Feedback & State Communication

**Principle**: Users must always know what state the app is in. Every state needs a distinct visual indicator.

### Required State Indicators

| State | Best-in-Class Pattern | What to Check |
|-------|----------------------|---------------|
| **Idle/Ready** | Claude Code: `? for shortcuts` below prompt. Codex: `100% context left` | Status bar shows idle hint |
| **Processing** | Claude Code: `Frolicking...` animated verb. Codex: `Planning (11s . esc to interrupt)` with live timer | Working indicator visible with interrupt hint |
| **Streaming** | All three: text appears character-by-character or in small chunks | Progressive rendering, not batch |
| **Selected item** | Claude Code: `>` prefix. OpenCode: `*` bullet. Codex: `>` prefix + `(current)` label | Selection indicator distinct from unselected |
| **Active panel** | Highlighted border, different color, or bold title | Active panel visually distinct from inactive |
| **Input mode** | Claude Code: prompt changes `>` to `!` or `&`. Codex: prompt changes | Current mode shown in prompt |
| **Error** | Claude Code: inline `User rejected update`. Codex: `[warning]` prefix. | Error text visible near the action that failed |
| **Completed action** | OpenCode: `Build . model . 5.1s`. Codex: `[checkmark] . 332ms` | Completion indicator with timing |

### Test Procedure

```bash
# Check idle state
tmux_assert_matches "$S" "shortcuts\|help\|ready\|commands" "idle state indicated"

# Check processing state — submit something, then immediately check
tmux_type "$S" "hello"
tmux_send "$S" Enter
tmux_wait_for_regex "$S" "thinking\|working\|loading\|processing\|[*]" 2
tmux_assert_matches "$S" "esc\|interrupt\|cancel" "interrupt hint shown during processing"
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Idle state has hint text | Shows available shortcuts/help | Blank or no indication of what to do |
| Processing shows indicator | Animated/live indicator + interrupt hint | Static text or no indicator |
| Selection is visually distinct | Highlighted, prefixed, or bold | Same style as unselected items |
| Error shows inline | Near the failed action with explanation | Only in stderr/logs, or silent |

---

## 4. Navigation & Escape Hygiene

**Principle**: Escape always goes "back." You must never get stuck in a state with no way out.

### Escape Key Contract

Every state must respond to Escape:

| State | Expected Escape Behavior |
|-------|-------------------------|
| Dialog/modal open | Close dialog, return to previous view |
| During processing | Interrupt/cancel the operation |
| Dropdown/autocomplete open | Close dropdown |
| Help overlay open | Close overlay |
| Nested dialog (step 2 of multi-step) | Go back to step 1 (not close entirely) |
| Prompt with text | Clear input (may require double-tap Esc Esc) |
| Prompt empty, no overlays | No-op (acceptable) or exit confirmation |

### Test Procedure

```bash
# Navigate to every reachable state and press Escape
# Track: does it return to a known parent state?

STATES_TO_TEST=(
    "help:?"
    "slash_menu:/"
    "file_picker:@"
)

for state_info in "${STATES_TO_TEST[@]}"; do
    IFS=: read -r state_name trigger <<< "$state_info"
    tmux_send "$S" "$trigger"
    sleep 0.3
    BEFORE=$(tmux_capture "$S")
    tmux_send "$S" Escape
    sleep 0.3
    AFTER=$(tmux_capture "$S")
    [ "$BEFORE" != "$AFTER" ] || echo "FAIL: Escape did nothing in $state_name"
done
```

### Modal Hierarchy

A well-designed TUI has a clear depth order:

```
Layer 0: Main view (always present)
Layer 1: Panels/sidebars (toggleable)
Layer 2: Overlays/dialogs (one at a time)
Layer 3: Confirmation dialogs (blocks until resolved)
Layer 4: Nested selection (within a dialog)
```

Escape should pop exactly one layer. Opening a new same-layer element should close the previous one.

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Escape from every overlay | Returns to parent state | Stuck in overlay or no-op |
| Escape from processing | Operation cancels | Operation continues |
| Double-Escape on empty prompt | Clears any residual state OR no-op | Exits the app without confirmation |
| Opening two overlays | Second replaces first OR second blocked | Both render on top of each other |
| Tab order between panels | Logical (left-to-right, top-to-bottom) | Random or broken |

---

## 5. Feedback Loops & Submission Flow

**Principle**: When a user submits input, the response must be immediate and multi-stage: (1) input clears, (2) loading appears, (3) content streams, (4) completion indicator.

### The Submission Timeline

```
User presses Enter
  |
  +-- [<100ms] Input field clears
  +-- [<100ms] User message appears in conversation (with > prefix)
  +-- [<200ms] Loading/working indicator appears
  |     Claude Code: "Frolicking..."
  |     OpenCode: thinking block with | margin
  |     Codex: "* Working (0s . esc to interrupt)"
  +-- [<500ms] First content streams in (or tool call begins)
  +-- [ongoing] Content renders progressively
  +-- [end] Completion indicator
        Claude Code: response ends, prompt returns
        OpenCode: "[square] Build . model . 5.1s"
        Codex: "[checkmark] . 332ms"
```

### Test Procedure

```bash
# Submit a message and verify the feedback chain
BEFORE=$(tmux_capture "$S")
tmux_type "$S" "hello world"
tmux_send "$S" Enter

# Check 1: Input should clear quickly
sleep 0.2
tmux_assert_not_contains "$S" "hello world" "input cleared after submit"

# Check 2: User message should appear in history
tmux_assert_contains "$S" "hello world" "user message shown in conversation"

# Check 3: Working indicator should appear
tmux_wait_for_regex "$S" "thinking\|working\|loading\|frolicking\|[*]" 3
echo "PASS: working indicator visible"

# Check 4: Interrupt hint should be shown
tmux_assert_matches "$S" "esc\|interrupt" "interrupt hint during processing"
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Input clears on submit | Gone within 200ms | Stays visible for >500ms |
| User message echoed | Appears in conversation thread | Lost / not shown |
| Loading state visible | Indicator appears within 500ms | Blank screen while processing |
| Streaming response | Text appears progressively | Long delay then full text |
| Completion indicator | Shows duration or completion marker | Just stops with no signal |

---

## 6. Error, Empty & Edge States

**Principle**: Every state the UI can be in must have a designed appearance. Blank space is a bug.

### Empty States

| Context | Best-in-Class | Anti-Pattern |
|---------|--------------|--------------|
| No sessions | Claude Code: `No recent activity` | Blank list |
| No search results | Codex: `no matches` | Empty dropdown with no text |
| Cleared conversation | Claude Code: `(no content)` | Blank screen |
| No config file | Codex: `Agents.md: <none>` | Missing field or error |
| First launch | OpenCode: rotating tips + placeholder `Ask anything...` | Blank screen with cursor |

### Error Display

| Pattern | Good | Bad |
|---------|------|-----|
| **Location** | Inline near the action that failed | Only in stderr or logs |
| **Prefix** | Distinct symbol: `[warning]`, `Error:`, red color | Same style as normal text |
| **Context** | What failed + why + what to do | Just "Error" with no detail |
| **Persistence** | Stays visible until acknowledged | Flashes and disappears |

### Test Procedure

```bash
# Test empty state: search for nonexistent text
tmux_type "$S" "/nonexistent_command_xyz"
sleep 0.3
FRAME=$(tmux_capture "$S")
# Should show "no matches" or "not found" — not blank
echo "$FRAME" | grep -qiE "no match|not found|no result|empty" \
    && echo "PASS: empty state handled" \
    || echo "WARN: check for empty state design"

# Test error handling: try invalid action
# (context-specific — depends on what the TUI does)
```

---

## 7. Layout & Resize Behavior

**Principle**: The TUI must be usable at 80x24 (minimum standard terminal) and scale gracefully to 200x50+.

### Size Tiers

| Size | Expected Behavior | Reference |
|------|-------------------|-----------|
| **80x24** | Core functionality preserved. Optional panels hidden. Truncation with `...` | Claude Code: LayoutMode::Compact |
| **120x30** | Standard layout. All panels visible. | Claude Code: LayoutMode::Standard |
| **160x40** | Extra space used for wider panels, more context | Claude Code: LayoutMode::Wide |
| **200x50+** | Ultrawide. Sidebars, extra columns | Claude Code: LayoutMode::Ultrawide |

### Test Procedure

```bash
SIZES=("80 24" "120 30" "160 40" "200 50")
for size in "${SIZES[@]}"; do
    read -r w h <<< "$size"
    tmux resize-pane -t "$S" -x "$w" -y "$h"
    sleep 0.5
    FRAME=$(tmux_capture "$S")

    # Check 1: No panic / crash
    tmux_is_alive "$S" || echo "FAIL: crashed at ${w}x${h}"

    # Check 2: Core UI elements still visible
    echo "$FRAME" | grep -qE ".\{10,\}" \
        && echo "PASS: content visible at ${w}x${h}" \
        || echo "FAIL: blank at ${w}x${h}"

    # Check 3: No overlapping (heuristic: lines shouldn't have garbled unicode)
    echo "$FRAME" | grep -qP '[\x00-\x08]' \
        && echo "WARN: possible corruption at ${w}x${h}" \
        || echo "PASS: no corruption at ${w}x${h}"
done
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| 80x24 usable | Core elements visible, input works | Crash, blank screen, or unusable |
| Resize doesn't crash | App continues running | Panic or exit |
| No overlapping elements | Clean frame at all sizes | Garbled text or overwritten content |
| Scroll indicators | Shown when content overflows | Content silently cut off |
| Width adaptation | Columns/panels adjust | Fixed width causes horizontal overflow |

---

## 8. Keyboard Design & Discoverability

**Principle**: All features must be reachable by keyboard. Shortcuts must not conflict with each other or with text input. Users must be able to discover shortcuts.

### Conflict Zones

| Key | Reserved By | TUI Must Not Override |
|-----|-------------|----------------------|
| `Ctrl+C` | Terminal (SIGINT) | Can override only with explicit exit handling |
| `Ctrl+Z` | Terminal (SIGTSTP) | Can override to suspend/undo |
| `Ctrl+D` | Terminal (EOF) | Should not override in input mode |
| `Ctrl+\` | Terminal (SIGQUIT) | Must not override |
| `Ctrl+S` | Terminal (XOFF) | Safe to override if raw mode |
| `Ctrl+Q` | Terminal (XON) | Safe to override if raw mode |

### Discoverability Patterns

| Method | Example | When to Use |
|--------|---------|-------------|
| **Status bar hint** | `? for shortcuts` (Claude Code/Codex) | Always visible, low distraction |
| **Command palette** | `Ctrl+P` (OpenCode) | Many commands, searchable |
| **Help overlay** | `?` or `F1` | Full reference, dismissible |
| **Contextual hints** | `Press enter to confirm . esc to cancel` (Codex) | In dialogs and modals |
| **Inline hints** | `tab agents  ctrl+p commands` (OpenCode) | Near the input area |

### Chord System Evaluation

If the TUI uses chord bindings (like OpenCode's `Ctrl+X` prefix):
- There must be a visual indicator that the prefix was pressed ("Waiting for next key...")
- Timeout should cancel the chord (don't stay in prefix mode forever)
- The prefix key itself must not conflict with text input

### Test Procedure

```bash
# Check: help is accessible
tmux_send "$S" "?"
if ! tmux_wait_for_regex "$S" "help\|shortcuts\|keybind" 2; then
    tmux_send "$S" F1
    if ! tmux_wait_for_regex "$S" "help\|shortcuts\|keybind" 2; then
        echo "FAIL: no discoverable help"
    fi
fi
tmux_send "$S" Escape

# Check: status bar shows hints
FRAME=$(tmux_capture "$S")
echo "$FRAME" | grep -qiE "shortcuts\|help\|commands\|ctrl" \
    && echo "PASS: hint text in status bar" \
    || echo "WARN: no hint text visible"
```

---

## 9. Permission & Confirmation Flows

**Principle**: Destructive actions must show a preview and require explicit confirmation. The user must be able to accept, reject, or amend.

### What Best-in-Class Does

**Claude Code's edit permission dialog**:
```
Edit file
main.py
-------
1  def greet(name):
2 +    """Docstring."""
3      return f"Hello, {name}!"
-------
Do you want to make this edit?
> 1. Yes
  2. Yes, allow all edits (shift+tab)
  3. No

Esc to cancel . Tab to amend
```

Key patterns:
- Unified diff preview with line numbers and `+`/`-` markers
- Three-option response: accept, auto-accept, reject
- Amendment option (Tab) lets user modify the proposed change
- Escape = cancel (same as reject)

**Codex's multi-step selection**:
- Step 1: Choose model → Step 2: Choose reasoning level
- Escape from step 2 goes back to step 1 (not close entirely)
- `(current)` marker shows active selection
- `(default)` marker shows recommended option

### Test Procedure

```bash
# If the TUI has file edit capabilities, trigger an edit and check:
# 1. Diff is shown before applying
# 2. User can accept (y/Enter) or reject (n/Escape)
# 3. Rejection is clearly communicated
# 4. Accept is clearly communicated
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Preview before action | Diff or summary shown | Action applied without preview |
| Clear accept/reject | Explicit options visible | Ambiguous or no reject option |
| Rejection feedback | "Rejected" or "Cancelled" shown | Silent — user unsure if it was applied |
| Accept feedback | Change applied + confirmation | Applied silently |
| Multi-step flows | Back goes to previous step | Back closes entire flow |
| Auto-accept option | Available for batch workflows | Every action requires individual approval |

---

## 10. Visual Design & Color

**Principle**: Color communicates meaning. The TUI should use a deliberate, restrained palette where every color choice serves a purpose. Screenshots are the primary evidence for this dimension.

### What Best-in-Class Does

**Claude Code**:
- Muted dark background with high-contrast text
- Green (`✓`) for success, red (`✗`) for errors, yellow for warnings
- Cyan/blue for interactive elements (links, selections)
- Dim gray for secondary information (timestamps, metadata)
- Bold white for user input, regular weight for AI responses
- No gratuitous color — most text is default foreground

**OpenCode**:
- Dark theme with `┃` colored left-margin lines (green for user, blue for AI)
- Token count and model info in muted colors
- Build status uses semantic colors (green checkmark, red X)
- Thinking/reasoning blocks get a distinct background shade

**Codex**:
- Minimal color palette: mostly white on dark
- `(current)` label in parentheses rather than color for selection state
- Green for success indicators, yellow for warnings
- Elapsed time counters in muted tone
- Strong visual hierarchy through indentation and symbols, not color alone

### Screenshot Protocol

Take screenshots using `tmux_screenshot` and inspect with the Read tool:

```bash
# Capture key states for visual review
tmux_screenshot "$S" "01-initial"        # First impression
tmux_screenshot "$S" "02-help"           # Help overlay
tmux_screenshot "$S" "04-processing"     # During streaming
tmux_screenshot "$S" "05-response"       # Completed response
tmux_screenshot "$S" "06-error"          # Error state

# Capture at all 4 terminal sizes
tmux_screenshot_sizes "$S" "09-resize"

# CRITICAL: Read each PNG with the Read tool to visually inspect
# Example: Read tool on /tmp/tui-review-screenshots/tui-review-01-initial.png
```

### Color Palette Assessment

When visually reviewing screenshots, categorize each color used:

| Role | Expected Color | Anti-Pattern |
|------|---------------|-------------|
| **Default text** | White or light gray on dark bg | Bright white everywhere (no hierarchy) |
| **Success/active** | Green (ANSI 32 or RGB green) | Green used for non-success elements |
| **Error/danger** | Red (ANSI 31 or RGB red) | Red used for decorative elements |
| **Warning/pending** | Yellow/amber (ANSI 33) | No distinct warning color |
| **Interactive/accent** | Cyan or blue (ANSI 36/34) | Same color as default text |
| **Muted/secondary** | Dim gray (ANSI 90 or dim) | Same brightness as primary text |
| **Selection highlight** | Inverse, bold, or distinct bg | Only a `>` prefix with no color change |
| **Borders/chrome** | Dim or muted | Same brightness as content text |

### Programmatic Checks

```bash
# Must-pass: TUI uses color at all
tmux_assert_not_monochrome "$S" "TUI uses color"

# Should-pass: at least 4 distinct colors (hierarchy)
tmux_assert_min_colors "$S" 4 "sufficient color palette"

# Semantic color checks (adapt to the specific TUI)
tmux_assert_text_color "$S" "●" "32" "active indicator is green"
tmux_assert_has_color "$S" "31" "red color available for errors"
tmux_assert_has_color "$S" "33" "yellow/amber for warnings"
tmux_assert_has_color "$S" "90" "dim gray for muted text"
```

### Pass/Fail Criteria

| Check | Pass | Fail |
|-------|------|------|
| Semantic color usage | Green=success, red=error, yellow=warn consistently | Colors used arbitrarily or inconsistently |
| Color variety | 4+ distinct colors creating visual hierarchy | Monochrome or only 1-2 colors |
| Contrast | All text readable against background | Light text on light bg, or dark on dark |
| Selection visibility | Selected item visually distinct (color, bold, inverse) | Only a `>` prefix, same color as unselected |
| Muted secondary info | Timestamps, metadata, hints dimmer than primary | Everything same brightness |
| Color restraint | Limited palette, each color has a role | Rainbow effect, too many competing colors |
| Dark theme consistency | Consistent dark background, no bright white blocks | Mixed light/dark regions without purpose |
| Border color | Borders dimmer than content | Borders same brightness as text |

### Common Color Anti-Patterns

- **Monochrome syndrome**: Entire TUI is white-on-black with no color differentiation
- **Rainbow explosion**: Every element a different color with no semantic meaning
- **Invisible selection**: Selected item looks identical to unselected items
- **Bright borders**: Box-drawing characters in bright white dominating the visual hierarchy
- **Missing error color**: Errors displayed in same color as normal text
- **Status ambiguity**: Success and error states use the same color
- **Low contrast muted text**: "Dim" text so dim it's unreadable (ANSI 90 on some dark themes)
