"""Strict, no-dispatch replay of the opt-in youki invocation journal.

This validates captured journal bytes, not their acquisition or Machine owner.
The caller must independently bind enrollment/session/boot, protected file
metadata, source/runtime digests and capture windows to the exact Machine.
Successful detached exec is runtime admission, not payload completion. Docker
exec kill/delete may be direct containerd actions, with no youki invocation.
"""
from __future__ import annotations

import hashlib
import json
import re

ENROLLMENT_LIMIT = 512
EVENT_LIMIT = 2048
JOURNAL_LIMIT = 16 * 1024 * 1024
UINT64 = (1 << 64) - 1
OPERATIONS = frozenset(('create', 'start', 'kill', 'delete', 'state', 'checkpoint',
                        'events', 'exec', 'features', 'list', 'pause', 'ps',
                        'resume', 'run', 'spec', 'update', 'info', 'completion',
                        'help', 'version'))
CONTAINER_OPERATIONS = OPERATIONS - {'features', 'list', 'spec', 'info',
                                    'completion', 'help', 'version'}
KEYS = frozenset(('schema_version', 'sequence', 'event', 'session_id', 'boot_id',
                  'invocation_id', 'operation', 'container_id', 'pid',
                  'starttime_ticks', 'monotonic_ns', 'wall_time_ns', 'outcome',
                  'exit_code'))
IDENTITY = ('session_id', 'boot_id', 'invocation_id', 'operation', 'container_id',
            'pid', 'starttime_ticks')


def require(value, message):
    if not value:
        raise ValueError('runtime audit: ' + message)


def integer(value, low=1, high=UINT64):
    return type(value) is int and low <= value <= high


def matches(value, pattern):
    return type(value) is str and re.fullmatch(pattern, value) is not None


def boot_id(value):
    return (matches(value, r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
            and value != '00000000-0000-0000-0000-000000000000')


def strict_json(raw, limit):
    require(type(raw) is bytes and 0 < len(raw) <= limit, 'invalid bounded JSON bytes')

    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result

    def constant(_):
        raise ValueError('runtime audit: nonfinite JSON constant')

    try:
        value = json.loads(raw.decode('ascii'), object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError('runtime audit: invalid JSON encoding or structure') from error
    require(type(value) is dict, 'JSON object required')
    return value


def enrollment(raw, *, expected_session_id, expected_boot_id):
    require(matches(expected_session_id, r'[0-9a-f]{64}'), 'invalid pinned session')
    require(boot_id(expected_boot_id), 'invalid pinned boot')
    value = strict_json(raw, ENROLLMENT_LIMIT)
    require(set(value) == {'schema_version', 'session_id', 'boot_id'}, 'enrollment fields')
    require(type(value['schema_version']) is int and value['schema_version'] == 1,
            'enrollment schema')
    require(value['session_id'] == expected_session_id and value['boot_id'] == expected_boot_id,
            'enrollment differs from independently pinned session/boot')
    return value


def validate(raw, *, enrollment_raw, status_raw, expected_session_id, expected_boot_id):
    """Validate an entire quiescent session, with every begin paired exactly once.

    No missing, empty, partial, duplicate, out-of-order or incomplete journal is
    accepted. Bounds apply to original bytes, including trailing newlines.
    A paired result records the runtime's outcome, not workload/cleanup success.
    """
    enrolled = enrollment(enrollment_raw, expected_session_id=expected_session_id,
                          expected_boot_id=expected_boot_id)
    require(type(status_raw) is bytes and status_raw == b'complete\n',
            'session is incomplete or status is malformed')
    require(type(raw) is bytes and 0 < len(raw) <= JOURNAL_LIMIT, 'journal byte bound')
    require(raw.endswith(b'\n'), 'partial final record')
    pending, seen, invocations = {}, set(), []
    last_monotonic = 0
    record_count = 0
    for index, line in enumerate(raw.split(b'\n')[:-1], 1):
        require(0 < len(line) + 1 <= EVENT_LIMIT and b'\r' not in line,
                'empty, oversized or noncanonical line framing')
        event = strict_json(line, EVENT_LIMIT - 1)
        require(set(event) == KEYS, 'event fields')
        require(type(event['schema_version']) is int and event['schema_version'] == 1,
                'event schema')
        require(integer(event['sequence']) and event['sequence'] == index, 'sequence gap/reorder')
        for key in ('session_id', 'boot_id'):
            require(event[key] == enrolled[key], 'foreign session/boot event')
        require(type(event['operation']) is str and event['operation'] in OPERATIONS,
                'unknown operation')
        cid = event['container_id']
        if event['operation'] in CONTAINER_OPERATIONS:
            require(matches(cid, r'[A-Za-z0-9_+.\-]{1,128}') and cid not in ('.', '..'),
                    'container operation without a safe typed ID')
        else:
            require(cid is None, 'non-container operation carries ID')
        require(integer(event['pid'], high=(1 << 32) - 1), 'PID type/range')
        for key in ('starttime_ticks', 'monotonic_ns', 'wall_time_ns'):
            require(integer(event[key]), key + ' type/range')
        require(event['monotonic_ns'] >= last_monotonic, 'monotonic journal order')
        last_monotonic = event['monotonic_ns']
        invocation = event['invocation_id']
        require(matches(invocation, r'[1-9][0-9]*:[1-9][0-9]*:[1-9][0-9]*'),
                'invocation identifier format')
        pid, birth, started = (int(part) for part in invocation.split(':'))
        require(pid == event['pid'] and birth == event['starttime_ticks'] and
                integer(started) and started <= event['monotonic_ns'],
                'invocation identifier differs from process birth/start')
        if event['event'] == 'begin':
            require(invocation not in seen and invocation not in pending, 'duplicate invocation')
            require(started == event['monotonic_ns'], 'begin timestamp differs from identity')
            require(event['outcome'] is None and event['exit_code'] is None, 'begin has outcome')
            pending[invocation] = event
        elif event['event'] == 'result':
            require(invocation in pending, 'orphan or duplicate result')
            begin = pending.pop(invocation)
            require(all(event[key] == begin[key] for key in IDENTITY), 'result identity drift')
            require(type(event['outcome']) is str and event['outcome'] in ('ok', 'error') and
                    integer(event['exit_code'], low=0, high=255), 'result outcome/code')
            payload_status = event['operation'] in ('exec', 'run')
            if event['outcome'] == 'error':
                # Setup failures return main's Result error (1); exec/run's
                # dispatch errors use the existing explicit process::exit(-1).
                require(event['exit_code'] in ((1, 255) if payload_status else (1,)),
                        'runtime error status differs from typed dispatch')
            elif not payload_status:
                require(event['exit_code'] == 0, 'non-payload operation has nonzero success status')
            seen.add(invocation)
            invocations.append({'begin': begin, 'result': event})
        else:
            raise ValueError('runtime audit: unknown event kind')
        record_count = index
    require(not pending, 'unmatched begin: interrupted or unfinished runtime')
    require(invocations, 'no paired invocations')
    # Results can finish out of admission order under concurrent Docker clients.
    invocations.sort(key=lambda pair: pair['begin']['sequence'])
    return {
        'schema_version': 1,
        'scope': 'raw_youki_journal_consistency_not_authenticated_Machine_or_workload_proof',
        'session_id': enrolled['session_id'], 'boot_id': enrolled['boot_id'],
        'record_count': record_count, 'invocation_count': len(invocations),
        'journal_bytes': len(raw), 'journal_sha256': hashlib.sha256(raw).hexdigest(),
        'enrollment_sha256': hashlib.sha256(enrollment_raw).hexdigest(),
        'status_sha256': hashlib.sha256(status_raw).hexdigest(),
        'invocations': invocations,
        'machine_binding_certified': False, 'full_process_absence_certified': False,
        'docker_operation_mapping_certified': False, 'runtime_invocation_certified': False,
    }
