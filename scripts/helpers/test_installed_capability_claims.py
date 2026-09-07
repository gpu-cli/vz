"""Adversarial tests for the capability-claims linter; synthetic surfaces are never evidence.

python3 -B -m unittest scripts/helpers/test_installed_capability_claims.py
"""
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import installed_capability_claims as claims
import installed_capability_matrix as matrix

LIVE = "macos-arm64/linux/*,macos-arm64/macos/developer"


def definitions_markdown(document):
    lines = ["<!-- capability-matrix: definitions -->"]
    for status in matrix.STATUSES:
        lines.append(f"- **{status}**: {document['status_definitions'][status]}")
    return "\n".join(lines) + "\n"


class ClaimsLinterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = matrix.load_json(matrix.ROOT, matrix.MATRIX)
        cls.removal = matrix.load_json(matrix.ROOT, claims.REMOVAL)
        cls.matrix = claims.Matrix(cls.document)
        cls.pattern = claims.removed_root_pattern(cls.removal["removed_roots"])

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="claims-"))
        self.addCleanup(shutil.rmtree, self.root, True)

    def lint(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return claims.check_surface(self.matrix, self.pattern, self.root, relative)

    def assertViolation(self, report, fragment):
        self.assertTrue(any(fragment in violation for violation in report.violations),
                        f"no violation mentions {fragment!r}: {report.violations}")

    def assertClean(self, report):
        self.assertEqual(report.violations, [], report.violations)

    def test_checked_in_surfaces_pass(self):
        out = io.StringIO()
        self.assertEqual(claims.run(matrix.ROOT, out=lambda line: out.write(line + "\n")), 0, out.getvalue())
        self.assertIn(f"{len(self.document['generated_surfaces'])} surfaces", out.getvalue())

    def test_bound_claim_matches_matrix(self):
        report = self.lint("a.md", "| macOS | <!-- capability-matrix: macos-arm64/linux/* pair -->**DEV** |\n")
        self.assertClean(report)
        self.assertEqual(report.bound, 1)

    def test_bound_claim_contradicting_matrix_is_reported(self):
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* pair -->**ACTIVE**\n")
        self.assertViolation(report, "claim ACTIVE contradicts the matrix")
        self.assertViolation(report, "macos-arm64/linux/developer.pair=DEV")

    def test_unbound_token_in_table_row_is_reported(self):
        report = self.lint("a.md", "| macOS | Linux | **DEV** |\n")
        self.assertViolation(report, "a.md:1: unbound status claim DEV")

    def test_unbound_token_in_prose_is_reported(self):
        report = self.lint("a.md", "Native macOS flows are ACTIVE today.\n")
        self.assertViolation(report, "unbound status claim ACTIVE")

    def test_vocabulary_enumeration_is_exempt(self):
        report = self.lint("a.md", "Claims carry **ACTIVE**, **DEV**, or **PLANNED**; DEV or PLANNED; ACTIVE/DEV/PLANNED/NA.\n")
        self.assertClean(report)
        self.assertEqual(report.mentions, 9)

    def test_vocabulary_marker_exempts_window(self):
        report = self.lint("a.html", "<!-- capability-matrix: vocabulary -->\n<text>primitive ACTIVE</text>\n<text>DEV</text>\n")
        self.assertViolation(report, "a.html:3: unbound status claim DEV")
        self.assertEqual(report.mentions, 1)

    def test_stacked_markers_bind_in_order(self):
        text = ("<!-- capability-matrix: macos-arm64/macos/developer posix_pty -->\n"
                "<!-- capability-matrix: macos-arm64/linux/* posix_pty -->\n"
                "PTY is **DEV** on native macOS and **PLANNED** on Linux.\n")
        report = self.lint("a.md", text)
        self.assertClean(report)
        self.assertEqual(report.bound, 2)

    def test_marker_without_claim_is_reported(self):
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* pair -->\nNo label here.\n")
        self.assertViolation(report, "a.md:1: marker 'macos-arm64/linux/* pair' is followed by a line without a status claim")
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* pair -->\n\n**DEV**\n")
        self.assertViolation(report, "followed by a blank line")
        self.assertViolation(report, "a.md:3: unbound status claim DEV")
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* pair -->")
        self.assertViolation(report, "reaches end of file")

    def test_too_many_markers_for_one_line(self):
        text = ("<!-- capability-matrix: macos-arm64/linux/* pair -->\n"
                "<!-- capability-matrix: macos-arm64/linux/* pair -->\n"
                "**DEV**\n")
        report = self.lint("a.md", text)
        self.assertViolation(report, "a.md:2: marker")
        self.assertEqual(report.bound, 1)

    def test_html_attribute_marker(self):
        report = self.lint("a.html", '<td><span class="tag tag-dev" vz-capability="macos-arm64/linux/developer docker_engine">DEV</span></td>\n')
        self.assertClean(report)
        self.assertEqual(report.bound, 1)

    def test_rust_comment_marker(self):
        text = ("    // capability-matrix: macos-arm64/linux/*,macos-arm64/macos/developer pair\n"
                "    /// Execute in one selected Ready Machine (Linux and native macOS on Apple silicon; DEV).\n")
        report = self.lint("main.rs", text)
        self.assertClean(report)
        self.assertEqual(report.bound, 1)

    def test_profile_and_backend_aliases_resolve(self):
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/container docker_engine -->**NA**\n")
        self.assertClean(report)
        report = self.lint("a.md", "<!-- capability-matrix: backend:macos-vz pair -->DEV\n")
        self.assertClean(report)
        report = self.lint("a.md", "<!-- capability-matrix: backend:linux_native pair -->PLANNED\n")
        self.assertClean(report)

    def test_host_selectors(self):
        report = self.lint("a.md", "<!-- capability-matrix: host:macos-x86_64 -->**NA** Intel Macs\n")
        self.assertClean(report)
        report = self.lint("a.md", "<!-- capability-matrix: host:linux-* -->**DEV**\n")
        self.assertViolation(report, "host linux-arm64=PLANNED")
        report = self.lint("a.md", "<!-- capability-matrix: host:macos-arm64 posix_exec -->**DEV**\n")
        self.assertViolation(report, "host selectors take no capability")

    def test_topology_capability_and_prefix(self):
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/developer topology:receipts -->**DEV**\n")
        self.assertClean(report)
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* split_dns,tls_ingress -->**PLANNED**\n")
        self.assertClean(report)

    def test_unknown_selector_or_capability_is_reported(self):
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/developer teleport -->**DEV**\n")
        self.assertViolation(report, "unknown capability 'teleport'")
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/plan9/* pair -->**DEV**\n")
        self.assertViolation(report, "matches no matrix pair")
        report = self.lint("a.md", "<!-- capability-matrix: macos-arm64/linux/* -->**DEV**\n")
        self.assertViolation(report, "require a capability list")
        report = self.lint("a.md", "<!-- capability-matrix: backend:hyperv pair -->**PLANNED**\n")
        self.assertViolation(report, "names no matrix backend")

    def test_na_spellings_are_equivalent(self):
        report = self.lint("a.md", "| <!-- capability-matrix: macos-arm64/windows/* pair -->N/A |\n")
        self.assertClean(report)

    def test_markdown_code_spans_and_fences_are_literal(self):
        text = ("Write `<!-- capability-matrix: vocabulary -->` before a legend; `DEV_HELP` is an identifier.\n"
                "```text\n"
                "<!-- capability-matrix: definitions -->\n"
                "vz-capability=\"<selectors> <capabilities>\"\n"
                "```\n")
        report = self.lint("a.md", text)
        self.assertClean(report)
        report = self.lint("a.md", "```text\nlifecycle (DEV)\n```\n")
        self.assertViolation(report, "a.md:2: unbound status claim DEV")

    def test_definitions_block_verbatim(self):
        report = self.lint("a.md", definitions_markdown(self.document))
        self.assertClean(report)
        self.assertEqual(report.definitions, 4)
        changed = copy.deepcopy(self.document)
        changed["status_definitions"]["DEV"] = "in development"
        report = self.lint("a.md", definitions_markdown(changed))
        self.assertViolation(report, "a.md:3: definition of DEV differs from status_definitions")
        text = definitions_markdown(self.document).replace("- **NA**", "- **N/A**")
        self.assertClean(self.lint("a.md", text))
        text = "\n".join(definitions_markdown(self.document).splitlines()[:-1]) + "\n"
        report = self.lint("a.md", text)
        self.assertViolation(report, "definitions block does not define NA")
        report = self.lint("a.md", "<!-- capability-matrix: definitions -->\n\nprose\n")
        self.assertViolation(report, "not followed by status definitions")

    def test_definitions_block_wraps_and_ends_at_list_tag(self):
        wrapped = definitions_markdown(self.document).replace("Requires non-empty evidence.", "Requires\n  non-empty evidence.")
        self.assertClean(self.lint("a.md", wrapped))
        html_block = ["<!-- capability-matrix: definitions -->", "<ul>"]
        for status in matrix.STATUSES:
            html_block.append(f'<li><span class="tag">{status}</span> {self.document["status_definitions"][status]}</li>')
        html_block += ["</ul>", "<p>No entry is shipped. Not a claim.</p>", ""]
        report = self.lint("a.html", "\n".join(html_block))
        self.assertClean(report)
        self.assertEqual(report.definitions, 4)

    def test_removed_roots_presented_as_commands(self):
        report = self.lint("a.md", "Use `vz run cargo build` or\n```bash\n$ vz init\nvz status\n```\n")
        self.assertViolation(report, "a.md:1: removed CLI root presented as a command: 'vz run'")
        self.assertViolation(report, "a.md:3: removed CLI root presented as a command: 'vz init'")
        self.assertEqual(len(report.violations), 2)
        report = self.lint("a.html", '<code>vz init/vz run</code>\n<td class="t-mono">&amp;&amp; vz self-sign</td>\n<pre class="cmdline">\nvz logs\n</pre>\n')
        for root in ("init", "run", "self-sign", "logs"):
            self.assertViolation(report, f"'vz {root}'")
        report = self.lint("main.rs", '    "  vz run cargo build\\n",\n')
        self.assertViolation(report, "main.rs:1: removed CLI root")

    def test_bare_roots_and_retained_verbs_are_not_flagged(self):
        report = self.lint("a.md", "Retired roots include `run`, `init`, and `logs`; use `vz status`, `vz up`, `vz-macos-setup`.\nPlain prose saying vz run is not a command presentation.\n")
        self.assertClean(report)

    def test_removal_docs_are_exempt(self):
        report = self.lint(claims.REMOVAL_DOCS[0], "`vz run` was removed.\n")
        self.assertClean(report)

    def test_missing_surface_fails_run(self):
        shutil.copytree(matrix.ROOT / "config", self.root / "config")
        shutil.copytree(matrix.ROOT / "schemas", self.root / "schemas")
        out = io.StringIO()
        self.assertEqual(claims.run(self.root, out=lambda line: out.write(line + "\n")), 1)
        self.assertIn("generated surface is missing", out.getvalue())

    def test_invalid_matrix_fails_run(self):
        shutil.copytree(matrix.ROOT / "config", self.root / "config")
        changed = copy.deepcopy(self.document)
        changed["pairs"][0]["machine_capabilities"]["posix_exec"]["status"] = "ACTIVE"
        (self.root / matrix.MATRIX).write_text(json.dumps(changed), encoding="utf-8")
        out = io.StringIO()
        self.assertEqual(claims.run(self.root, out=lambda line: out.write(line + "\n")), 1)
        self.assertIn("matrix is invalid", out.getvalue())


if __name__ == "__main__":
    unittest.main()
