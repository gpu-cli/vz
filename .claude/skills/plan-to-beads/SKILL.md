---
name: plan-to-beads
description: |
  Convert a planning folder into well-structured Beads issues optimized for parallel agent execution via implement-bd.

  Use when asked to:
  - Import/convert a planning folder to Beads
  - Create issues/beads from planning docs
  - Bootstrap Beads from markdown plans
  - Set up work tracking for a planning folder
  - "Turn this plan into beads" or "create beads for planning/my-feature"
  - Break down a plan into parallelizable work items

  Reads planning folders (README.md, architecture.md, XX-*.md), extracts dependency graphs,
  and creates epics/features/tasks with correct parent-child relationships and dependency edges.
  Produces issues that implement-bd can pick up and execute, potentially in parallel.
---

# Plan to Beads

Convert a planning folder into Beads issues structured for parallel agent execution.

## Goal

Read a planning folder, understand the work and its dependencies, and produce a minimal set of well-scoped Beads issues that:
- Can be picked up by `implement-bd` agents
- Maximize parallelism (only add deps where truly required)
- Include plan references so agents have full context
- Include validation tasks so work is verified before dependents start

## Step 1: Read the Planning Folder

Read **every file** in the folder. Planning folders vary in structure:

```
planning/<topic>/
├── README.md              # Overview, order, success criteria, deps
├── architecture.md        # Design (optional)
├── 00-infrastructure.md   # Phase files (numbered, may or may not have -impl suffix)
├── 01-foundation.md
├── 02-feature-a-impl.md
└── ...
```

Variations to handle:
- Files may be `XX-name.md` or `XX-name-impl.md`
- Some folders have `architecture.md`, some don't
- Some folders are a single standalone `.md` file (no subfolder)
- Phase numbering may start at 00 or 01

**Read all files.** Extract from each:

| File | Extract |
|------|---------|
| README.md | Title, overview, phase list with deps, success criteria, key decisions |
| architecture.md | Design context (pass through to epic description) |
| XX-*.md | Phase title, purpose, steps (### Step N or ### sections), pre-conditions, validation/testing section, acceptance criteria |

## Step 2: Build the Dependency Graph

**Do not assume linear ordering.** Read the README's dependency section (often a mermaid diagram or text list) and each phase's "Depends on" lines. Build the actual graph:

```
# Example from company-agent-tui-v2:
# Phase 0 → Phase 1 → Phase 2 (needs Phase 0 too)
#                    → Phase 3 (parallel with 2)
#                    → Phase 4 (parallel with 2, 3)
# Phase 2,3,4 → Phase 5
```

Rules:
- If README says "Phase 3 and 4 can run in parallel" - no dep between them
- If a phase says "Depends on: Phase 1" - add that single dep, not all prior phases
- If no dependency info exists, fall back to sequential ordering by number
- Tasks within a phase are parallel by default unless a step says "after Step N"

## Step 3: Check for Duplicates

```bash
bd list --status=all 2>/dev/null
```

If issues with matching titles already exist, skip creating them. Warn the user about skipped duplicates.

## Step 4: Design the Issue Hierarchy

Before creating anything, plan the full hierarchy. Use this mapping:

| Planning Element | Beads Type | When |
|-----------------|------------|------|
| Planning folder | `epic` | Always 1 per folder (rarely 2+ if truly independent workstreams) |
| Phase file (XX-*.md) | `feature` | 1 per phase file |
| Step within phase | `task` | 1 per `### Step` or meaningful section |
| Phase validation | `task` | 1 per feature, always the last child |

### Task Scoping Guidelines

Each task should be **completable in a single implement-bd session** (~30-60 min of agent work). If a step in the plan is too large:
- Split into sub-tasks with clear boundaries
- Each sub-task should modify a bounded set of files
- Each sub-task should be independently testable

If a step is trivial (< 5 min), merge it with an adjacent step into one task.

### Validation Task Per Feature

Every feature gets a validation task as its last child. The validation task:
- Depends on all other tasks in that feature
- Contains specific test commands from the plan's validation section
- Specifies the testing approach based on component type

## Step 5: Create Issues

Create in this order: epic first, then features (capturing IDs), then tasks under each feature.

### Epic

```bash
bd create \
  --title="<Title from README>" \
  --type=epic \
  --priority=2 \
  --description="$(cat <<'EOF'
<Overview from README, 2-4 sentences>

## Success Criteria
<Checklist from README>

## Architecture
<Key decisions summary, 2-3 bullets - or "See architecture.md">

## Plan Reference
- planning/<folder>/README.md
- planning/<folder>/architecture.md
EOF
)"
```

Capture the epic ID from output.

### Features (one per phase)

```bash
bd create \
  --title="<Phase Title>" \
  --type=feature \
  --parent=<epic-id> \
  --priority=2 \
  --description="$(cat <<'EOF'
<Phase overview, 1-2 sentences>

## Scope
<What this phase delivers>

## Pre-conditions
<From phase file, if any>

## Plan Reference
- planning/<folder>/XX-phase.md
EOF
)"
```

Capture each feature ID. Title should be descriptive (not "Phase 1: Foundation" but "Zoom Navigation Foundation" or "Daemon Core with IPC").

### Tasks (one per step)

```bash
bd create \
  --title="<Descriptive action title>" \
  --type=task \
  --parent=<feature-id> \
  --priority=2 \
  --description="$(cat <<'EOF'
<What to implement and why, 1-3 sentences>

## Files
<Files to create/modify, from the plan>

## Approach
<Key implementation notes from the plan>

## Plan Reference
- planning/<folder>/XX-phase.md (Step N)
EOF
)"
```

Task titles should be action-oriented: "Implement ZoomLevel state machine", "Add BeadStore trait to agent-db", "Wire daemon to ActivityTracker".

### Validation Task

```bash
bd create \
  --title="Validate: <Feature Name>" \
  --type=task \
  --parent=<feature-id> \
  --priority=2 \
  --description="$(cat <<'EOF'
Validate the <feature> implementation.

## Test Commands
<Specific commands from the plan's validation section>

## Testing Approach
<Method based on component type - see table below>

## Expected Results
<Pass criteria>

## Plan Reference
- planning/<folder>/XX-phase.md (Validation section)
EOF
)"
```

Testing approach by component type:

| Component | Method | Notes |
|-----------|--------|-------|
| Rust library | `cargo nextest run -p <crate>` | Unit + integration tests (always scope with `-p`) |
| Rust CLI/TUI | `cargo nextest run -p <crate>` + tmux-cli-test skill | Snapshot + interactive tests |
| TypeScript | `bun test` + `bun run typecheck` | Type safety + unit tests |
| React/Next.js | Playwright MCP | E2E browser tests |
| API endpoints | curl/httpie | Request/response validation |
| gRPC | `cargo nextest run -p <crate>` + manual stream test | Stream behavior validation |

## Step 6: Set Up Dependencies

Apply the dependency graph from Step 2 using **feature-level deps** (not task-level across features):

```bash
# Feature-level: Phase 2 depends on Phase 1
bd dep add <phase-2-feature-id> <phase-1-feature-id>

# Within a feature: validation depends on all impl tasks
bd dep add <validate-task-id> <impl-task-1-id>
bd dep add <validate-task-id> <impl-task-2-id>
```

**Dependency rules:**
- Feature deps follow the graph from Step 2 (NOT always linear)
- Validation task depends on all sibling tasks in its feature
- Tasks within a feature are parallel unless plan says otherwise
- Do NOT create cross-feature task-level deps (use feature-level deps instead)

**Parallel features** (no dep between them) let multiple implement-bd agents work simultaneously.

## Step 7: Verify and Sync

```bash
# Check structure
bd show <epic-id> --children

# Verify parallel work exists
bd ready

# Check nothing is incorrectly blocked
bd blocked

# Sync to git
bd sync
```

**Verify:**
- `bd ready` shows tasks from independent features (parallel work available)
- `bd blocked` shows only tasks that genuinely need prior work
- Every feature has a validation task
- Epic has all features as children

## Step 8: Report

Print a summary:

```
Created Beads for: <planning folder>

Epic: <title> (<id>)
  Feature: <title> (<id>) - N tasks
  Feature: <title> (<id>) - N tasks [parallel with above]
  Feature: <title> (<id>) - N tasks [depends on <id>]
  ...

Parallelism: <N> features can start immediately
Total tasks: <N> (including <N> validation tasks)

Next: Run `bd ready` to see what's actionable, or use implement-bd to start working.
```

## Example

For `planning/company-agent-tui-v2/` with its dependency graph:

```
Epic: TUI v2 Zoom Interface
├── Feature: BeadStore Infrastructure        ← ready immediately
│   ├── Task: Add BeadsIssue type to agent-db
│   ├── Task: Implement BeadStore trait
│   └── Task: Validate: BeadStore Infrastructure
├── Feature: Zoom Navigation Foundation      ← depends on Infrastructure
│   ├── Task: Implement ZoomLevel state machine
│   ├── Task: Rewrite app.rs with zoom shell
│   ├── Task: Implement global + per-zoom keybindings
│   └── Task: Validate: Zoom Navigation
├── Feature: Fleet View                      ← depends on Infrastructure + Foundation
│   ├── Task: Implement beads tree widget
│   ├── Task: Add fleet command dispatch
│   └── Task: Validate: Fleet View
├── Feature: Agents View                     ← depends on Foundation only (PARALLEL with Fleet)
│   ├── Task: Implement session list from SessionStore
│   ├── Task: Add quick actions via factory_command
│   └── Task: Validate: Agents View
├── Feature: Session View                    ← depends on Foundation only (PARALLEL with Fleet, Agents)
│   ├── Task: Implement conversation renderer
│   ├── Task: Add input via ChatBackend
│   └── Task: Validate: Session View
└── Feature: Gates Overlay                   ← depends on ALL above
    ├── Task: Filter beads_issue by gate type
    ├── Task: Handle OpenCode permission requests
    └── Task: Validate: Gates Overlay
```

Parallelism: After Foundation completes, Fleet/Agents/Session can all run simultaneously with 3 agents.
