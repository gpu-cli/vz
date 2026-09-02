# Design — vz datasheet page

<!-- impeccable:design-schema 1 -->
<!-- Recorded from the built world at site/index.html. Ground truth over intention. -->

## World

**The Datasheet.** vz is specified like a semiconductor part, not marketed. The page is a printed component datasheet: part-number masthead, general description, typical-application schematic, absolute maximum ratings, pin configuration, truth table, ordering information, revision history, and numbered notes. The truth table is the pitch: every agent action resolves to `HOST EFFECT: NONE`.

## Surfaces

- `site/index.html` — the entire deliverable. Single static file, no build step. Persuade mode.

## Palette

| Token | Value | Role |
|---|---|---|
| `--paper` | `#f2efe6` | Ground: cream laid paper |
| `--paper-2` | `#ece7d9` | Recessed ground (scrollbar track) |
| `--ink` | `#1f1c18` | Warm ink: all text, rules, chip body strokes |
| `--ink-60` | `rgba(31,28,24,.68)` | Secondary text (≥4.5:1 on paper) |
| `--ink-45` | `rgba(31,28,24,.45)` | Decorative hairlines only, never text |
| `--rule` | `rgba(31,28,24,.28)` | 1px hairline rules and cell borders |
| `--rule-strong` | `#1f1c18` | 2px structural rules, table heads |
| `--blue` | `#0f4c81` | Engineering blue: measured/live data, ACTIVE tags, focus, links |
| `--red` | `#a33327` | Signal red: PLANNED/guarded states, dashed boundary elements |
| `--amber` | `#7a6015` | DEV tags (4.8:1 on paper) |

Color strategy: restrained — neutrals plus engineering blue carrying live data, red reserved for planned/guarded states. Committed flat print: zero radii, zero shadows, no gradients.

## Typography

| Face | Role |
|---|---|
| **Archivo** (wdth 62–125, wght 400–900) | Display: part number `vz` (wdth 112, wght 830, clamped 88–190px), section headings (wdth 104, wght 700), `NONE` verdict, run button. Load via Google Fonts. |
| **IBM Plex Sans** | Document body: description, clauses, table cells |
| **IBM Plex Mono** | All data: commands, doc-meta, tags, pins, figures, footer |

Type character: warm engineering documentation — Plex is the subject's native lettering, not a costume; Archivo expanded carries the semiconductor part-identity scale. Tracking floors: −0.035em part number, −0.015em section heads. Section numbers (2, 3, 4…) sit in blue Plex Mono inside the heading — the sequence is information (datasheet section numbering), not decoration.

## Layout

- Document frame: max-width 1180px, gutters clamp(20px, 4vw, 56px). One hairline grammar: 2px strong rules open sections, 1px rules divide rows.
- Masthead: giant part number left, document-meta table right; below 960px stacks.
- Two-column intro (features list 5fr / description 7fr); stacks below 960px.
- Clauses 5.1–5.6: 210px label column + body; single column below 960px, `min-width: 0` on body to prevent overflow.
- Data tables: `.tbl-wrap` scrolls horizontally rather than squashing columns; tabular numerals.
- Bench: controls column 5fr / readout grid 7fr; stacks to single column below 960px.

## Signature Interaction

**The boundary test bench (EVM-1).** An agent stimulus (`rm -rf /`, `curl | sh`, `cat ~/.ssh`, `apt-get install`, `docker compose up`) is selected and run; the command types into the guest console; readouts resolve: guest effect, host effect `NONE`, recovery. One flash animation on the host-effect cell per run. Header carries `ILLUSTRATIVE` — readouts are simulated, not measured. Reduced-motion renders instantly without typing.

## Motion

One authored moment: the bench run (typewriter console + cell flash), reduced-motion aware. Everything else is print-still. `scroll-behavior: smooth` only.

## Browser Surfaces

Selection: blue background, paper text. Caret: blue. Scrollbar: paper track, ink-hairline thumb with paper border. Focus: 2px blue outline, 2px offset. Links: blue, 1px underline, 3px offset; 2px on hover. Tabular numerals on all tables.

## States

- Preset buttons: default / hover (ink border) / `aria-pressed` (ink fill, paper text) / disabled n/a
- Run button: default / hover (blue fill) / `disabled` (55% opacity, wait cursor)
- Bench readouts: empty ("Awaiting stimulus") / loading ("Stimulus executing in guest…") / resolved
- Print: bench hidden, links ink-colored

## Facts & Claims Discipline

- Status tags carry truth: `ACTIVE` (shipped v0.3.20), `DEV` (in development), `PLANNED` (roadmap, "subject to change")
- ~3s boot cites repo docs (note 2); no independent benchmark claimed
- Bench labeled ILLUSTRATIVE
- No invented benchmarks/customers/pricing; revision history keeps future rows honest (`0.3.20` ACTIVE, `F.1` DEV, `F.2/F.3` PLANNED)

## Accessibility

- Semantic `main`/`header`/`section`/`table`/`footer`; `aria-labelledby` throughout; SVG schematics carry descriptive `aria-label`s
- Bench buttons in `role="group"`; readouts `aria-live="polite"`
- Contrast: body ≥4.5:1 (secondary 14.76:1, amber tags 4.8:1); large display ≥3:1
- `prefers-reduced-motion`: instant rendering, no smooth scroll

## Rasters

None. All imagery is inline SVG (schematic, IC package) — geometry the session specifies exactly; no photographic or generated rasters ship in this artifact.