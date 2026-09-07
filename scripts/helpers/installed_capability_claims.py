"""Lint capability/status claims in generated surfaces against the capability matrix.

config/host-target-capabilities-v0.4.json is the source of truth for every
ACTIVE/DEV/PLANNED/NA label a public surface carries. This module reads that
matrix, then reads every file listed in its ``generated_surfaces`` and checks
three explicit, documented claim patterns (stdlib only, Python 3.9+):

1. Status claims. Every occurrence of the tokens ``ACTIVE``, ``DEV``, ``PLANNED``,
   ``NA`` (or ``N/A``) in a surface is a claim and must be bound to matrix entries
   by an explicit marker that precedes it. Nothing is guessed from table
   headers or prose. A token without a marker is a violation unless it is a
   vocabulary mention (two or more distinct tokens joined only by ``,``, ``/``,
   ``or``, ``and``: "ACTIVE, DEV, or PLANNED"), sits inside a declared
   definitions block, or sits in the window of a ``vocabulary`` marker.

   Marker forms (identical grammar):

     <!-- capability-matrix: <selectors> <capabilities> -->   markdown/HTML
     vz-capability="<selectors> <capabilities>"               HTML attribute
     // capability-matrix: <selectors> <capabilities>         Rust source

   ``<selectors>`` is a comma-separated list of ``host/target/profile`` pair
   ids (each segment may be a glob such as ``macos-arm64/linux/*``; profile and
   backend aliases from ``vocabularies`` are accepted), ``host:<host-glob>``
   items (host status; no capability), or ``backend:<wire-name-or-alias>``
   items (every pair using that backend). ``<capabilities>`` is a
   comma-separated list of ``pair`` (the pair status), a machine capability, or
   a topology capability (optionally prefixed ``topology:``).

   Binding window: a marker binds the next status token after it on its own
   line, or on the next line that carries non-marker content when the marker
   line has none. Several marker-only lines stack and bind that content line's
   tokens in order. A marker that binds nothing, or a blank line after a
   pending marker, is a violation. The claim must equal the matrix status of
   every entry the marker selects.

   ``<!-- capability-matrix: vocabulary -->`` exempts the tokens in its window
   (legends, vocabulary lists). ``<!-- capability-matrix: definitions -->``
   opens a definitions block (rule 3). In markdown, inline code spans are
   literal text (neither markers nor claims) and fenced blocks cannot carry
   markers, so status claims belong outside fenced blocks.

2. Removed CLI roots. Any ``vz <root>`` for a root in
   config/cli-removal-v0.4.json ``removed_roots`` that is presented as a command
   (markdown code span or fenced block; HTML <code>, <pre>, or a ``t-mono`` /
   ``cmdline`` element; any line of a Rust help surface) is a violation. Only
   the explicit removal documents may show them.

3. Status vocabulary definitions. A definitions block (the non-blank lines
   after a ``definitions`` marker, ending at a blank line or a closing
   ``</ul>``/``</ol>``/``</dl>``) must define all four statuses, one item per
   line with wrapped continuation lines allowed, and each definition text must
   equal ``status_definitions`` verbatim after HTML tags, entities, markdown
   emphasis/backticks and whitespace runs are normalised.

  python3 -B scripts/check-capability-claims.py
"""
from __future__ import annotations

import argparse
import fnmatch
import html
from pathlib import Path
import re
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import installed_capability_matrix as matrix_module

ROOT = matrix_module.ROOT
MATRIX = matrix_module.MATRIX
REMOVAL = "config/cli-removal-v0.4.json"
REMOVAL_DOCS = ("planning/developer-environments/legacy-cli-removal.md", REMOVAL)
MAX_SURFACE = 8 * 1024 * 1024

STATUSES = matrix_module.STATUSES
TOKEN_RE = re.compile(r"(?<![A-Za-z0-9_])(N/A|ACTIVE|DEV|PLANNED|NA)(?![A-Za-z0-9_])")
_SEP = r"(?:\*\*|`|</?(?:b|strong|em|code|span)\b[^>]*>|\s)*"
_TOKEN = r"(?:N/A|ACTIVE|DEV|PLANNED|NA)"
_JOIN = r"(?:,\s*(?:or|and)\b|,|/|\bor\b|\band\b)"
MENTION_RE = re.compile(rf"{_TOKEN}(?:{_SEP}{_JOIN}{_SEP}{_TOKEN})+")
MARKER_RE = re.compile(
    r"capability-matrix:[ \t]*([A-Za-z0-9_*,:/.-]+)(?:[ \t]+([A-Za-z0-9_*,:]+))?"
    r"|vz-capability=\"([^\"]*)\""
)
COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
DEFINITION_ITEM_RE = re.compile(r"^(N/A|ACTIVE|DEV|PLANNED|NA)\b\s*[:—–-]?\s*(.*)$")
FENCE_RE = re.compile(r"^\s*(```|~~~)")
LIST_END_RE = re.compile(r"^\s*</(?:ul|ol|dl)>\s*$")
CODE_SPAN_RE = re.compile(r"`([^`\n]+)`")
HTML_CODE_REGION_RE = re.compile(
    r"<pre\b[^>]*>.*?</pre>|<code\b[^>]*>.*?</code>"
    r"|<(td|span|div|p)\b[^>]*class=\"[^\"]*\b(?:t-mono|cmdline)\b[^\"]*\"[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)
SELECTOR_ITEM_RE = re.compile(r"^(?:host:([A-Za-z0-9_*.-]+)|backend:([A-Za-z0-9_*.-]+)|([A-Za-z0-9_*.-]+)/([A-Za-z0-9_*.-]+)/([A-Za-z0-9_*.-]+))$")


class InvalidInput(Exception):
    """Raised when the matrix, removal inventory, or a surface cannot be read."""


def normalise_status(token: str) -> str:
    return "NA" if token == "N/A" else token


def plain_text(line: str) -> str:
    text = COMMENT_RE.sub(" ", line)
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = text.replace("**", "").replace("`", "")
    return " ".join(text.split())


class Matrix:
    """Resolved view of the capability matrix for claim binding."""

    def __init__(self, document: Dict[str, Any]) -> None:
        self.document = document
        self.definitions: Dict[str, str] = document["status_definitions"]
        self.surfaces: List[str] = list(document["generated_surfaces"])
        self.hosts: Dict[str, str] = {name: host["status"] for name, host in document["hosts"].items()}
        vocab = document["vocabularies"]
        self.machine_capabilities: List[str] = list(vocab["machine_capabilities"])
        self.topology_capabilities: List[str] = list(vocab["topology_capabilities"])
        self.profile_aliases: Dict[str, str] = dict(vocab["profiles"]["aliases"])
        self.backend_aliases: Dict[str, str] = dict(vocab["backends"]["aliases"])
        self.backends: List[str] = list(vocab["backends"]["wire_names"])
        self.pairs: List[Dict[str, Any]] = list(document["pairs"])

    def resolve_selectors(self, text: str) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
        """Return (pairs, hosts, errors) selected by a comma-separated selector list."""
        pairs: List[Dict[str, Any]] = []
        hosts: List[str] = []
        errors: List[str] = []
        for item in filter(None, text.split(",")):
            match = SELECTOR_ITEM_RE.match(item)
            if not match:
                errors.append(f"unrecognised selector {item!r}")
                continue
            host_glob, backend, host, target, profile = match.groups()
            if host_glob is not None:
                selected = [name for name in self.hosts if fnmatch.fnmatchcase(name, host_glob)]
                if not selected:
                    errors.append(f"host selector {item!r} matches no matrix host")
                hosts.extend(selected)
                continue
            if backend is not None:
                wire = self.backend_aliases.get(backend, backend)
                if wire not in self.backends and not any(fnmatch.fnmatchcase(name, wire) for name in self.backends):
                    errors.append(f"backend selector {item!r} names no matrix backend or alias")
                    continue
                selected = [pair for pair in self.pairs
                            if pair.get("backend") is not None and fnmatch.fnmatchcase(pair["backend"], wire)]
                if not selected:
                    errors.append(f"backend selector {item!r} matches no pair")
                pairs.extend(selected)
                continue
            profile = self.profile_aliases.get(profile, profile)
            selected = [pair for pair in self.pairs
                        if fnmatch.fnmatchcase(pair["host"], host)
                        and fnmatch.fnmatchcase(pair["target"], target)
                        and fnmatch.fnmatchcase(pair["profile"], profile)]
            if not selected:
                errors.append(f"pair selector {item!r} matches no matrix pair")
            pairs.extend(selected)
        return pairs, hosts, errors

    def entry_status(self, pair: Dict[str, Any], capability: str) -> Optional[str]:
        if capability == "pair":
            return pair["pair_status"]
        if capability.startswith("topology:"):
            capability = capability[len("topology:"):]
            return pair["topology_capabilities"][capability]["status"] if capability in self.topology_capabilities else None
        if capability in self.machine_capabilities:
            return pair["machine_capabilities"][capability]["status"]
        if capability in self.topology_capabilities:
            return pair["topology_capabilities"][capability]["status"]
        return None


class Marker:
    def __init__(self, kind: str, selectors: str, capabilities: Optional[str], line: int) -> None:
        self.kind = kind  # "bind", "vocabulary", "definitions"
        self.selectors = selectors
        self.capabilities = capabilities
        self.line = line

    def describe(self) -> str:
        if self.kind != "bind":
            return self.kind
        return f"{self.selectors} {self.capabilities or ''}".strip()


def parse_marker(match: "re.Match[str]", line_number: int) -> Marker:
    if match.group(3) is not None:
        parts = match.group(3).split()
        selectors = parts[0] if parts else ""
        capabilities = parts[1] if len(parts) > 1 else None
    else:
        selectors, capabilities = match.group(1), match.group(2)
    if selectors in ("vocabulary", "definitions") and capabilities is None:
        return Marker(selectors, selectors, None, line_number)
    return Marker("bind", selectors, capabilities, line_number)


class SurfaceReport:
    def __init__(self, relative: str) -> None:
        self.relative = relative
        self.bound = 0
        self.mentions = 0
        self.definitions = 0
        self.violations: List[str] = []

    def fail(self, line: int, message: str) -> None:
        self.violations.append(f"{self.relative}:{line}: {message}")


def check_binding(matrix: Matrix, marker: Marker, status: str, line: int, report: SurfaceReport) -> None:
    pairs, hosts, errors = matrix.resolve_selectors(marker.selectors)
    for error in errors:
        report.fail(line, f"marker {marker.describe()!r}: {error}")
    if errors:
        return
    mismatches: List[str] = []
    if hosts:
        if pairs or marker.capabilities not in (None, "status"):
            report.fail(line, f"marker {marker.describe()!r}: host selectors take no capability and cannot mix with pairs")
            return
        for host in hosts:
            if matrix.hosts[host] != status:
                mismatches.append(f"host {host}={matrix.hosts[host]}")
    else:
        if not marker.capabilities:
            report.fail(line, f"marker {marker.describe()!r}: pair selectors require a capability list (use 'pair' for the pair status)")
            return
        capabilities = [name for name in marker.capabilities.split(",") if name]
        for capability in capabilities:
            for pair in pairs:
                actual = matrix.entry_status(pair, capability)
                if actual is None:
                    report.fail(line, f"marker {marker.describe()!r}: unknown capability {capability!r}")
                    return
                if actual != status:
                    mismatches.append(f"{pair['host']}/{pair['target']}/{pair['profile']}.{capability}={actual}")
    if mismatches:
        report.fail(line, f"claim {status} contradicts the matrix ({marker.describe()}): " + "; ".join(sorted(set(mismatches))))
    else:
        report.bound += 1


def check_definitions(matrix: Matrix, lines: Sequence[str], start: int, report: SurfaceReport) -> int:
    """Validate the definitions block beginning at index ``start``; return the index after it."""
    items: List[Tuple[int, str, str]] = []
    index = start
    while index < len(lines) and lines[index].strip():
        if LIST_END_RE.match(lines[index]):
            index += 1
            break
        text = plain_text(BULLET_RE.sub("", lines[index], count=1))
        if text:
            match = DEFINITION_ITEM_RE.match(text)
            if match:
                items.append((index + 1, normalise_status(match.group(1)), match.group(2)))
            elif items:
                line_no, status, body = items[-1]
                items[-1] = (line_no, status, f"{body} {text}".strip())
        index += 1
    if not items:
        report.fail(start, "definitions marker is not followed by status definitions")
        return index
    seen: Dict[str, int] = {}
    for line_no, status, body in items:
        if status in seen:
            report.fail(line_no, f"status {status} is defined twice in one definitions block")
            continue
        seen[status] = line_no
        expected = " ".join(matrix.definitions[status].split())
        if " ".join(body.split()) != expected:
            report.fail(line_no, f"definition of {status} differs from status_definitions; expected: {expected}")
        else:
            report.definitions += 1
    for status in STATUSES:
        if status not in seen:
            report.fail(start, f"definitions block does not define {status}")
    return index


def literal_spans(line: str, fenced: bool) -> List[Tuple[int, int]]:
    """Spans where marker syntax is literal text (markdown code), never a marker."""
    if fenced:
        return [(0, len(line))]
    return [(m.start(), m.end()) for m in CODE_SPAN_RE.finditer(line)]


def check_status_claims(matrix: Matrix, lines: Sequence[str], report: SurfaceReport, markdown: bool = False) -> None:
    pending: List[Marker] = []
    fence: Optional[str] = None
    index = 0
    while index < len(lines):
        line = lines[index]
        line_no = index + 1
        fence_match = FENCE_RE.match(line) if markdown else None
        if fence_match and (fence is None or fence_match.group(1) == fence):
            fence = None if fence else fence_match.group(1)
        literal = literal_spans(line, fence is not None) if markdown else []
        markers = [m for m in MARKER_RE.finditer(line)
                   if not any(start <= m.start() < end for start, end in literal)]
        events: List[Tuple[int, str, Any]] = []
        for match in markers:
            events.append((match.start(), "marker", parse_marker(match, line_no)))
        mention_spans = [(m.start(), m.end()) for m in MENTION_RE.finditer(line)]
        code_spans = [(m.start(), m.end()) for m in CODE_SPAN_RE.finditer(line)] if markdown and fence is None else []
        for match in TOKEN_RE.finditer(line):
            if any(m.start() <= match.start() < m.end() for m in markers):
                continue
            if any(start <= match.start() < end for start, end in code_spans):
                continue
            mention = any(start <= match.start() < end for start, end in mention_spans)
            events.append((match.start(), "token", (normalise_status(match.group(1)), mention)))
        events.sort(key=lambda event: event[0])
        definitions_start: Optional[int] = None
        for _, kind, payload in events:
            if kind == "marker":
                marker: Marker = payload
                if marker.kind == "definitions":
                    definitions_start = index + 1
                elif pending and any(item.kind != marker.kind for item in pending):
                    report.fail(line_no, "vocabulary markers cannot be mixed with binding markers")
                else:
                    pending.append(marker)
                continue
            status, mention = payload
            if pending and pending[0].kind == "vocabulary":
                report.mentions += 1
                continue
            if pending:
                check_binding(matrix, pending.pop(0), status, line_no, report)
                continue
            if mention:
                report.mentions += 1
                continue
            report.fail(line_no, f"unbound status claim {status}; precede it with a capability-matrix marker")
        residual = line
        for match in reversed(markers):
            residual = residual[:match.start()] + " " + residual[match.end():]
        residual = re.sub(r"^\s*//\s*$", "", residual)
        content = plain_text(residual)
        blank = not line.strip()
        if blank or content:
            for marker in pending:
                if marker.kind == "bind":
                    where = "a blank line" if blank else "a line without a status claim"
                    report.fail(marker.line, f"marker {marker.describe()!r} is followed by {where}")
            pending = []
        if definitions_start is not None:
            index = check_definitions(matrix, lines, definitions_start, report)
            continue
        index += 1
    for marker in pending:
        report.fail(marker.line, f"marker {marker.describe()!r} reaches end of file without a status claim")


def removed_root_pattern(roots: Sequence[str]) -> re.Pattern:
    alternatives = "|".join(re.escape(root) for root in sorted(roots, key=len, reverse=True))
    return re.compile(rf"(?<![A-Za-z0-9_-])vz\s+({alternatives})(?![A-Za-z0-9_-])")


def command_regions(relative: str, text: str) -> Iterable[Tuple[int, str]]:
    """Yield (line_number, fragment) pairs where text is presented as a command."""
    suffix = Path(relative).suffix
    if suffix == ".rs":
        for number, line in enumerate(text.splitlines(), 1):
            yield number, line
        return
    if suffix in (".html", ".htm"):
        for match in HTML_CODE_REGION_RE.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            for offset, fragment in enumerate(html.unescape(TAG_RE.sub(" ", match.group(0))).splitlines()):
                yield line_no + offset, fragment
        return
    fence: Optional[str] = None
    for number, line in enumerate(text.splitlines(), 1):
        fence_match = FENCE_RE.match(line)
        if fence_match and (fence is None or fence_match.group(1) == fence):
            fence = None if fence else fence_match.group(1)
            continue
        if fence:
            yield number, line
            continue
        for span in CODE_SPAN_RE.finditer(line):
            yield number, span.group(1)
        for match in re.finditer(r"<code\b[^>]*>(.*?)</code>", line):
            yield number, html.unescape(match.group(1))


def check_removed_roots(pattern: re.Pattern, relative: str, text: str, report: SurfaceReport) -> None:
    if relative in REMOVAL_DOCS:
        return
    for line_no, fragment in command_regions(relative, text):
        for match in pattern.finditer(fragment):
            report.fail(line_no, f"removed CLI root presented as a command: 'vz {match.group(1)}'")


def read_surface(root: Path, relative: str) -> List[str]:
    path = root / relative
    if not path.is_file():
        raise InvalidInput(f"{relative}: generated surface is missing")
    if path.stat().st_size > MAX_SURFACE:
        raise InvalidInput(f"{relative}: exceeds {MAX_SURFACE} bytes")
    return path.read_text(encoding="utf-8").splitlines()


def check_surface(matrix: Matrix, pattern: re.Pattern, root: Path, relative: str) -> SurfaceReport:
    report = SurfaceReport(relative)
    lines = read_surface(root, relative)
    check_status_claims(matrix, lines, report, markdown=Path(relative).suffix in (".md", ".markdown"))
    check_removed_roots(pattern, relative, "\n".join(lines) + "\n", report)
    return report


def load(root: Path, matrix_relative: str = MATRIX, removal_relative: str = REMOVAL) -> Tuple[Matrix, re.Pattern]:
    try:
        document = matrix_module.load_json(root, matrix_relative)
        removal = matrix_module.load_json(root, removal_relative)
    except matrix_module.InvalidMatrix as error:
        raise InvalidInput(str(error)) from error
    violations = matrix_module.validate_matrix(document)
    if violations:
        raise InvalidInput(f"{matrix_relative}: matrix is invalid; run scripts/check-host-target-capabilities.py")
    roots = removal.get("removed_roots") if isinstance(removal, dict) else None
    if not isinstance(roots, list) or not all(isinstance(root_name, str) and root_name for root_name in roots):
        raise InvalidInput(f"{removal_relative}: removed_roots must be a list of command roots")
    return Matrix(document), removed_root_pattern(roots)


def run(root: Path = ROOT, matrix_relative: str = MATRIX, removal_relative: str = REMOVAL,
        out: Callable[[str], None] = print) -> int:
    try:
        matrix, pattern = load(root, matrix_relative, removal_relative)
        reports = [check_surface(matrix, pattern, root, relative) for relative in matrix.surfaces]
    except InvalidInput as error:
        out(f"FAIL {error}")
        return 1
    total_violations = 0
    for report in reports:
        for violation in report.violations:
            out(f"FAIL {violation}")
        total_violations += len(report.violations)
        state = "ok  " if not report.violations else "FAIL"
        out(f"{state} {report.relative}: {report.bound} claims bound, {report.mentions} vocabulary mentions, "
            f"{report.definitions} definitions verified, {len(report.violations)} violations")
    out(f"{matrix_relative}: {len(reports)} surfaces, {sum(r.bound for r in reports)} claims bound, "
        f"{total_violations} violations")
    return 1 if total_violations else 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root (default: this checkout)")
    parser.add_argument("--matrix", default=MATRIX, help="matrix path relative to --root")
    parser.add_argument("--removal", default=REMOVAL, help="CLI removal inventory relative to --root")
    args = parser.parse_args(argv)
    return run(args.root.resolve(), args.matrix, args.removal)


if __name__ == "__main__":
    sys.exit(main())
