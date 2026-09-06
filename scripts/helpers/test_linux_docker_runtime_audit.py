"""Synthetic journal adversaries, not native/runtime/Machine evidence."""
import copy
import hashlib
import json
import unittest
from unittest import mock

import linux_docker_runtime_audit as audit

SESSION = 'a' * 64
BOOT = 'b1234567-1111-2222-3333-0123456789ab'
CID = 'c' * 64
ENROLLED = json.dumps({'schema_version': 1, 'session_id': SESSION, 'boot_id': BOOT}).encode()


def record(sequence, kind='begin', *, pid=100, start=1000, now=None, operation='create',
           cid=CID, outcome='ok', code=0):
    return {'schema_version': 1, 'sequence': sequence, 'event': kind,
            'session_id': SESSION, 'boot_id': BOOT,
            'invocation_id': f'{pid}:123:{start}', 'operation': operation,
            'container_id': cid, 'pid': pid, 'starttime_ticks': 123,
            'monotonic_ns': start if now is None else now, 'wall_time_ns': 2000000 + sequence,
            'outcome': None if kind == 'begin' else outcome,
            'exit_code': None if kind == 'begin' else code}


def paired(**kwargs):
    return [record(1, **kwargs), record(2, 'result', now=1001, **kwargs)]


def packed(events):
    return b''.join(json.dumps(event, separators=(',', ':')).encode() + b'\n' for event in events)


def validate(events=None, **kwargs):
    options = {'enrollment_raw': ENROLLED, 'status_raw': b'complete\n',
               'expected_session_id': SESSION, 'expected_boot_id': BOOT}
    options.update(kwargs)
    raw = options.pop('raw', packed(paired() if events is None else events))
    return audit.validate(raw, **options)


class RuntimeAuditTests(unittest.TestCase):
    def test_exact_raw_hashes_pairing_and_no_broad_claims(self):
        proof = validate()
        self.assertEqual(proof['record_count'], 2)
        self.assertEqual(proof['invocation_count'], 1)
        self.assertEqual(proof['journal_sha256'], hashlib.sha256(packed(paired())).hexdigest())
        self.assertEqual(proof['enrollment_sha256'], hashlib.sha256(ENROLLED).hexdigest())
        self.assertEqual(proof['status_sha256'], hashlib.sha256(b'complete\n').hexdigest())
        for key in ('machine_binding_certified', 'full_process_absence_certified',
                    'docker_operation_mapping_certified', 'runtime_invocation_certified'):
            self.assertIs(proof[key], False)

    def test_concurrent_invocations_may_finish_out_of_begin_order(self):
        events = [record(1), record(2, pid=101, start=1001),
                  record(3, 'result', pid=101, start=1001, now=1002),
                  record(4, 'result', now=1003)]
        proof = validate(events)
        self.assertEqual([pair['begin']['pid'] for pair in proof['invocations']], [100, 101])
        self.assertEqual([pair['result']['sequence'] for pair in proof['invocations']], [4, 3])

    def test_all_typed_operations_and_payload_nonzero_success(self):
        for operation in audit.OPERATIONS:
            cid = CID if operation in audit.CONTAINER_OPERATIONS else None
            outcomes = [('ok', 0), ('error', 1)]
            if operation in ('exec', 'run'):
                outcomes += [('ok', 37), ('ok', 137), ('error', 255)]
            for outcome, code in outcomes:
                with self.subTest(operation=operation, outcome=outcome, code=code):
                    self.assertEqual(validate(paired(operation=operation, cid=cid,
                                                     outcome=outcome, code=code))['invocation_count'], 1)

    def test_impossible_typed_dispatch_exit_codes_rejected(self):
        for operation in audit.OPERATIONS:
            cid = CID if operation in audit.CONTAINER_OPERATIONS else None
            outcomes = [('error', 0), ('error', 37), ('error', 137)]
            if operation not in ('exec', 'run'):
                outcomes += [('ok', 37), ('ok', 137), ('error', 255)]
            for outcome, code in outcomes:
                with self.subTest(operation=operation, outcome=outcome, code=code), self.assertRaises(ValueError):
                    validate(paired(operation=operation, cid=cid, outcome=outcome, code=code))

    def test_enrollment_exact_fields_types_and_independent_pins(self):
        for kwargs in ({'expected_session_id': 'b' * 64}, {'expected_boot_id': BOOT.replace('b', 'c')},
                       {'expected_session_id': True}, {'expected_boot_id': '0' * 36},
                       {'expected_boot_id': '00000000-0000-0000-0000-000000000000'}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                validate(**kwargs)
        original = json.loads(ENROLLED)
        for field, value in (('schema_version', True), ('schema_version', 1.0),
                             ('session_id', 'A' * 64), ('boot_id', BOOT.upper()),
                             ('extra', 'secret')):
            changed = dict(original, **{field: value})
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate(enrollment_raw=json.dumps(changed).encode())
        for raw in (b'{}', b'[]', b'null', b'\xff', b'{"schema_version":1,"schema_version":1}',
                    ENROLLED + b' ' * audit.ENROLLMENT_LIMIT):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate(enrollment_raw=raw)

    def test_sticky_status_and_bounded_original_bytes(self):
        for status in (b'incomplete\n', b'complete', b'complete\n\n', b'complete\x00',
                       b'complete\nincomplete\n', 'complete\n', None):
            with self.subTest(status=status), self.assertRaises(ValueError):
                validate(status_raw=status)
        raw = packed(paired())
        with mock.patch.object(audit, 'JOURNAL_LIMIT', len(raw)):
            self.assertEqual(validate(raw=raw)['record_count'], 2)
            with self.assertRaises(ValueError):
                validate(raw=raw + b'\n')
        for malformed in (b'', raw[:-1], raw + b' ', raw + b'\n', raw.replace(b'\n', b'\r\n'),
                          raw.decode(), b'\xff\n', b'[]\n', b'null\n', b'NaN\n'):
            with self.subTest(raw=malformed), self.assertRaises(ValueError):
                validate(raw=malformed)

    def test_event_boundary_includes_original_newline(self):
        events = paired()
        lines = packed(events).splitlines()
        padded = lines[0] + b' ' * (audit.EVENT_LIMIT - len(lines[0]) - 1) + b'\n' + lines[1] + b'\n'
        self.assertEqual(validate(raw=padded)['record_count'], 2)
        with self.assertRaises(ValueError):
            validate(raw=b' ' + padded)

    def test_duplicate_unknown_missing_json_keys_and_secret_fields(self):
        raw = packed(paired())
        with self.assertRaises(ValueError):
            validate(raw=raw.replace(b'"sequence":1', b'"sequence":1,"sequence":1', 1))
        for key in audit.KEYS:
            events = paired()
            del events[0][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                validate(events)
        for key in ('argv', 'env', 'spec', 'error', 'path', 'args'):
            events = paired()
            events[0][key] = 'SECRET_CANARY'
            with self.subTest(extra=key), self.assertRaises(ValueError):
                validate(events)

    def test_integer_types_ranges_and_nonfinite_json_rejected(self):
        for key in ('schema_version', 'sequence', 'pid', 'starttime_ticks', 'monotonic_ns', 'wall_time_ns'):
            for value in (True, False, 1.0, '1', None, -1, 0, audit.UINT64 + 1):
                events = paired()
                events[0][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    validate(events)
        for value in (True, 0.0, '0', None, -1, 256, float('nan'), float('inf')):
            events = paired()
            events[1]['exit_code'] = value
            with self.subTest(code=value), self.assertRaises(ValueError):
                validate(events)
        events = paired(pid=1 << 32)
        with self.assertRaises(ValueError):
            validate(events)

    def test_container_ids_and_noncontainer_command_separation(self):
        for cid in (None, '', '.', '..', '../host', 'secret\nline', '\u00e9', 'a' * 129, 123):
            with self.subTest(cid=cid), self.assertRaises(ValueError):
                validate(paired(cid=cid))
        for cid in ('a', 'BuildKit_1-2.3+4', 'a' * 128):
            self.assertEqual(validate(paired(cid=cid))['invocation_count'], 1)
        for operation in audit.OPERATIONS - audit.CONTAINER_OPERATIONS:
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                validate(paired(operation=operation))
        for operation in ('runc', 'CREATE', '', None, [], {}):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                validate(paired(operation=operation))

    def test_sequence_gaps_reorder_duplicates_and_partial_pairs(self):
        original = paired()
        candidates = ([original[0]], [original[1]], list(reversed(original)), original + original,
                      [original[0], dict(original[1], sequence=3)],
                      [original[0], dict(original[1], event='begin')],
                      [original[0], original[1], dict(original[1], sequence=3)],
                      [original[0], original[1], dict(original[0], sequence=3, monotonic_ns=1002)])
        for events in candidates:
            with self.subTest(events=events), self.assertRaises(ValueError):
                validate(copy.deepcopy(events))

    def test_identity_birth_pid_reuse_and_result_drift(self):
        for key, value in (('session_id', 'b' * 64), ('boot_id', BOOT.replace('b', 'c')),
                           ('operation', 'delete'), ('container_id', 'd' * 64),
                           ('pid', 101), ('starttime_ticks', 124),
                           ('invocation_id', '100:123:999')):
            events = paired()
            events[1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate(events)
        for value in ('0100:123:1000', '100:0123:1000', '100:123:01000', '0:123:1000',
                      '100:123:1002', '100:123:-1', '100:123:1e3', '100:123:1000:1', None):
            events = paired()
            for event in events:
                event['invocation_id'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate(events)

    def test_monotonic_order_and_wall_clock_adjustment(self):
        events = paired()
        events[1]['monotonic_ns'] = 999
        with self.assertRaises(ValueError):
            validate(events)
        events = paired()
        events[1]['wall_time_ns'] = 1  # CLOCK_REALTIME can step; not monotonic evidence.
        self.assertEqual(validate(events)['invocation_count'], 1)
        events = paired()
        events[0]['monotonic_ns'] = 999
        with self.assertRaises(ValueError):
            validate(events)

    def test_result_semantics_and_no_payload_completion_inference(self):
        for field, value in (('event', 'end'), ('outcome', None), ('outcome', 'success'),
                             ('outcome', True)):
            events = paired()
            events[1][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate(events)
        for field, value in (('outcome', 'ok'), ('exit_code', 0)):
            events = paired()
            events[0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate(events)
        with self.assertRaises(ValueError):
            validate(paired(outcome='error', code=0))
        proof = validate(paired(operation='exec'))
        self.assertFalse(proof['docker_operation_mapping_certified'])
        self.assertFalse(proof['full_process_absence_certified'])


if __name__ == '__main__':
    unittest.main()
