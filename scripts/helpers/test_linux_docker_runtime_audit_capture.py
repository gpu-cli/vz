"""Inert protocol/command adversaries; no guest or enrollment is dispatched."""
import base64
import copy
import hashlib
import json
import unittest

import linux_docker_runtime_audit_capture as capture

SESSION = 'a' * 64
RUNTIME = 'b' * 64
BOOT = 'c1234567-1111-2222-3333-0123456789ab'
ENROLLMENT = json.dumps({'schema_version': 1, 'session_id': SESSION, 'boot_id': BOOT}, separators=(',', ':')).encode() + b'\n'


def encoded(raw):
    return base64.b64encode(raw)


def journal():
    common = {'schema_version': 1, 'session_id': SESSION, 'boot_id': BOOT,
              'invocation_id': '10:20:30', 'operation': 'version', 'container_id': None,
              'pid': 10, 'starttime_ticks': 20}
    return b''.join(json.dumps(dict(common, sequence=seq, event=event,
                                   monotonic_ns=30 + seq - 1, wall_time_ns=100 + seq,
                                   outcome=None if seq == 1 else 'ok', exit_code=None if seq == 1 else 0),
                              separators=(',', ':')).encode() + b'\n'
                    for seq, event in ((1, 'begin'), (2, 'result')))


def snapshot_raw(events=b'', *, status=b'complete\n', enrollment=ENROLLMENT):
    rows = [b'VZ_RUNTIME_AUDIT_SNAPSHOT_V1', BOOT.encode(), RUNTIME.encode(), encoded(enrollment), encoded(status)]
    for index, size in enumerate((4096, len(enrollment), len(events), len(status))):
        rows.append(('%x|0|0|%d|5|%d|%d|100|101' %
                     (0o40700 if index == 0 else 0o100600, 2 if index == 0 else 1, index + 10, size)).encode())
    return b'\n'.join(rows + [hashlib.sha256(events).hexdigest().encode(), b'END', b''])


def parse(raw, **kwargs):
    return capture.parse_snapshot(raw, session_id=SESSION, runtime_sha256=RUNTIME, **kwargs)


def chunk_raw(raw_snapshot, events, index):
    chunk = events[index * capture.CHUNK_SIZE:(index + 1) * capture.CHUNK_SIZE]
    return b'\n'.join((b'VZ_RUNTIME_AUDIT_CHUNK_V1', str(index).encode(), encoded(raw_snapshot),
                       encoded(chunk), encoded(raw_snapshot), b'END', b''))


class CaptureTests(unittest.TestCase):
    def test_empty_enrollment_and_complete_capture(self):
        enrolled = parse(snapshot_raw(), enrolled=True)
        events = journal()
        raw = snapshot_raw(events)
        snapshot = parse(raw, expected_boot_id=BOOT)
        capture.same_enrollment(enrolled, snapshot)
        chunk = capture.parse_chunk(chunk_raw(raw, events, 0), snapshot=snapshot, index=0)
        result = capture.assemble(snapshot, [chunk], copy.deepcopy(snapshot))
        self.assertEqual(result['events'], events)
        self.assertEqual(result['enrollment'], ENROLLMENT)
        self.assertEqual(result['status'], b'complete\n')
        self.assertEqual(result['validation']['record_count'], 2)
        self.assertFalse(result['validation']['full_process_absence_certified'])

    def test_source_selected_mutation_only_fresh_enrollment(self):
        script = capture.enrollment_script(SESSION, RUNTIME)
        self.assertIn('test ! -e "$root" && test ! -L "$root" || fail', script)
        self.assertIn('"$bb" mkdir -m 700 "$root"\nset -C', script)
        self.assertLess(script.index(': > "$root/events.jsonl"'), script.index('> "$root/enrollment.json"'))
        self.assertLess(script.index('sync "$root/events.jsonl"'), script.index('> "$root/enrollment.json"'))
        self.assertNotIn('mkdir -p', script)
        self.assertNotIn('rm ', script)
        self.assertNotIn('flock', script)
        self.assertNotIn(' --version', script)
        for value in (capture.snapshot_script(SESSION, RUNTIME), capture.chunk_script(SESSION, RUNTIME, 0, 5, 'd' * 64)):
            self.assertNotIn('mkdir', value)
            self.assertNotIn(' > "$root', value)
            self.assertIn('test "$before_events" = "$(meta "$root/events.jsonl")"', value)
            self.assertIn('test ! -L "$runtime"', value)

    def test_source_inputs_reject_injection_and_boolean_counters(self):
        for session, runtime in (('x;exit', RUNTIME), (SESSION, RUNTIME + '\n'), (True, RUNTIME)):
            with self.assertRaises(ValueError):
                capture.enrollment_script(session, runtime)
        for index, size, sha in ((True, 1, RUNTIME), (-1, 1, RUNTIME), (16, capture.JOURNAL_LIMIT, RUNTIME),
                                 (0, True, RUNTIME), (0, capture.JOURNAL_LIMIT + 1, RUNTIME),
                                 (1, 5, RUNTIME), (0, 1, 'x')):
            with self.assertRaises(ValueError):
                capture.chunk_script(SESSION, RUNTIME, index, size, sha)

    def test_snapshot_framing_and_base64_rejects_trailing_duplicate_and_noncanonical(self):
        raw = snapshot_raw()
        for changed in (raw[:-1], raw + b'\n', raw + raw, raw.replace(b'END\n', b'OTHER\n'), b'x' * 8193,
                        raw.replace(encoded(ENROLLMENT), encoded(ENROLLMENT) + b'='),
                        raw.replace(encoded(ENROLLMENT), b'!')):
            with self.subTest(changed=changed[:40]), self.assertRaises(ValueError):
                parse(changed)

    def test_session_boot_runtime_and_status_binding(self):
        for raw in (snapshot_raw(status=b'incomplete\n'), snapshot_raw(status=b'complete'),
                    snapshot_raw().replace(SESSION.encode(), b'wrong'),
                    snapshot_raw(enrollment=ENROLLMENT.replace(SESSION.encode(), b'd' * 64)),
                    snapshot_raw().replace(BOOT.encode(), b'00000000-0000-0000-0000-000000000000'),
                    snapshot_raw().replace(RUNTIME.encode(), b'c' * 64)):
            if raw == snapshot_raw():
                continue
            with self.assertRaises(ValueError):
                parse(raw)
        with self.assertRaises(ValueError):
            parse(snapshot_raw(), expected_boot_id=BOOT.replace('c', 'd'))
        with self.assertRaises(ValueError):
            parse(snapshot_raw(journal()), enrolled=True)

    def test_protected_file_metadata_and_aliases(self):
        original = snapshot_raw().split(b'\n')
        for row_index in range(5, 9):
            for field, value in ((0, b'a180'), (1, b'1'), (2, b'1'), (4, b'6'), (5, b'0'),
                                 (7, b'0'), (8, b'0'), (1, b'00')):
                rows = list(original)
                fields = rows[row_index].split(b'|')
                fields[field] = value
                rows[row_index] = b'|'.join(fields)
                with self.subTest(row=row_index, field=field), self.assertRaises(ValueError):
                    parse(b'\n'.join(rows))
        for row_index in (6, 7, 8):
            rows = list(original)
            fields = rows[row_index].split(b'|')
            fields[3] = b'2'
            rows[row_index] = b'|'.join(fields)
            with self.assertRaises(ValueError):
                parse(b'\n'.join(rows))

    def test_metadata_sizes_and_journal_bound(self):
        for row_index, size in ((6, 1), (8, 1), (7, capture.JOURNAL_LIMIT + 1)):
            rows = snapshot_raw().split(b'\n')
            fields = rows[row_index].split(b'|')
            fields[6] = str(size).encode()
            rows[row_index] = b'|'.join(fields)
            with self.assertRaises(ValueError):
                parse(b'\n'.join(rows))

    def test_enrollment_to_capture_allows_only_event_growth(self):
        initial, final = parse(snapshot_raw()), parse(snapshot_raw(journal()))
        capture.same_enrollment(initial, final)
        for name in final['metadata']:
            for field in (0, 1, 2, 3, 4, 5, 7, 8):
                if name == 'events' and field in (7, 8):
                    continue
                changed = copy.deepcopy(final)
                changed['metadata'][name][field] += 1
                with self.assertRaises(ValueError):
                    capture.same_enrollment(initial, changed)

    def test_multichunk_exact_bound_and_cross_chunk_hash(self):
        events = b'x' * capture.CHUNK_SIZE + b'y' * 37
        raw = snapshot_raw(events)
        snapshot = parse(raw)
        chunks = [capture.parse_chunk(chunk_raw(raw, events, index), snapshot=snapshot, index=index) for index in (0, 1)]
        self.assertEqual(b''.join(chunks), events)
        self.assertLess(len(chunk_raw(raw, events, 0)), 4 * 1024 * 1024)
        with self.assertRaises(ValueError):
            capture.assemble(snapshot, [chunks[0][:-1] + b'z', chunks[1]], snapshot)

    def test_chunk_truncation_sequence_metadata_and_final_change(self):
        events = journal()
        raw = snapshot_raw(events)
        snapshot = parse(raw)
        valid = chunk_raw(raw, events, 0)
        for changed in (valid[:-1], valid + b'\n', valid.replace(b'\n0\n', b'\n1\n'),
                        valid.replace(encoded(events), encoded(events[:-1])),
                        valid.replace(encoded(raw), encoded(snapshot_raw(events + b' ')), 1)):
            with self.assertRaises(ValueError):
                capture.parse_chunk(changed, snapshot=snapshot, index=0)
        final = copy.deepcopy(snapshot)
        final['metadata']['events'][8] += 1
        with self.assertRaises(ValueError):
            capture.assemble(snapshot, [events], final)

    def test_assemble_requires_complete_inventory_and_all_journal_semantics(self):
        events = journal()
        snapshot = parse(snapshot_raw(events))
        for chunks in ([], [events, events], [events[:-1]], [bytearray(events)]):
            with self.assertRaises(ValueError):
                capture.assemble(snapshot, chunks, snapshot)
        for bad in (b'', events.split(b'\n')[0] + b'\n', events[:-1], events + b'{}\n'):
            snapshot = parse(snapshot_raw(bad))
            with self.assertRaises(ValueError):
                capture.assemble(snapshot, [bad] if bad else [], snapshot)


if __name__ == '__main__':
    unittest.main()
