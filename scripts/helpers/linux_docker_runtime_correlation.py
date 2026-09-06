"""Pure candidate correlation, not authentication of Docker command receipts.

The caller must derive requests from independently replayed command/inspect
receipts and authenticated Engine SystemTime guards on the journal's Machine.
These are guest wall-clock windows, never host command timestamps. A unique
candidate is a match under those supplied constraints, not causal proof that
the Docker client initiated it. In particular, overlapping healthcheck and
explicit exec invocations cannot be distinguished by this journal schema.

No generation/init PID, payload status, signal forwarding, cleanup completion,
or full process/history claim is inferred. Runtime PID/birth belongs to youki.
"""
import re

import linux_docker_runtime_audit as audit

MAX_REQUESTS = 256
MAX_WINDOW_NS = 120 * 1_000_000_000
RELATIONS = {
    'start': frozenset(('create', 'start')),
    'run': frozenset(('create', 'start')),
    'restart': frozenset(('create', 'start')),
    'exec': frozenset(('exec',)),
    # Engine metadata create is not OCI task create. Likewise, task cleanup
    # need not occur during Docker rm, nor does every signal invoke youki kill.
    'create': frozenset(),
    'inspect': frozenset(),
    'logs': frozenset(),
    'wait': frozenset(),
    'stop': frozenset(),
    'kill': frozenset(),
    'rm': frozenset(),
}
REQUEST_KEYS = frozenset(('request_id', 'container_id', 'docker_operation',
                          'runtime_operation', 'engine_before_ns', 'engine_after_ns'))


def require(value, reason):
    if not value:
        raise ValueError('runtime correlation: ' + reason)


def _request(value):
    require(type(value) is dict and set(value) == REQUEST_KEYS, 'request fields')
    require(type(value['request_id']) is str and
            re.fullmatch('[a-z][a-z0-9-]{0,79}', value['request_id']), 'request identity')
    require(type(value['container_id']) is str and
            re.fullmatch('[0-9a-f]{64}', value['container_id']), 'exact Docker container ID')
    operation = value['docker_operation']
    require(type(operation) is str and operation in RELATIONS, 'unknown Docker operation')
    selected = value['runtime_operation']
    if RELATIONS[operation]:
        require(type(selected) is str and selected in RELATIONS[operation], 'unsupported runtime relation')
    else:
        require(selected is None, 'no direct runtime relation for Docker operation')
    before, after = value['engine_before_ns'], value['engine_after_ns']
    require(audit.integer(before) and audit.integer(after) and
            0 < after - before <= MAX_WINDOW_NS, 'invalid bounded Engine clock window')
    return dict(value)


def _reference(pair):
    begin, result = pair['begin'], pair['result']
    return {'invocation_id': begin['invocation_id'], 'container_id': begin['container_id'],
            'operation': begin['operation'], 'begin_sequence': begin['sequence'],
            'result_sequence': result['sequence'], 'pid': begin['pid'],
            'starttime_ticks': begin['starttime_ticks'],
            'begin_wall_time_ns': begin['wall_time_ns'], 'result_wall_time_ns': result['wall_time_ns'],
            'begin_monotonic_ns': begin['monotonic_ns'], 'result_monotonic_ns': result['monotonic_ns'],
            'outcome': result['outcome'], 'runtime_exit_code': result['exit_code']}


def correlate(raw, *, enrollment_raw, status_raw, expected_session_id,
              expected_boot_id, requests):
    """Validate all bytes, then find candidates for externally selected windows.

    Each required relation needs at least one fully contained invocation. Multiple
    candidates are retained as ambiguous, never selected by success/exit code.
    A candidate cannot serve two requests, even when either request is ambiguous.
    Unmatched invocations remain explicit background/unattributed observations.
    No-operation requests require runtime_operation=None and make no zero-call
    assertion: unrelated background work may still occur in their windows.
    """
    require(type(requests) is list and 0 < len(requests) <= MAX_REQUESTS, 'bounded nonempty requests')
    selected = [_request(value) for value in requests]
    require(len({value['request_id'] for value in selected}) == len(selected), 'duplicate request identity')
    journal = audit.validate(raw, enrollment_raw=enrollment_raw, status_raw=status_raw,
                             expected_session_id=expected_session_id, expected_boot_id=expected_boot_id)
    # Clock steps would invalidate an interval interpretation. The underlying
    # journal remains potentially valid; this stricter correlation is refused.
    clock = sorted((event['sequence'], event['wall_time_ns'])
                   for pair in journal['invocations'] for event in (pair['begin'], pair['result']))
    require(all(left[1] <= right[1] for left, right in zip(clock, clock[1:])),
            'guest wall clock regressed; correlation unproven')
    references = [_reference(pair) for pair in journal['invocations']]
    used, matches = set(), []
    for request in selected:
        operation = request['runtime_operation']
        candidates = []
        if operation is not None:
            candidates = [row for row in references
                          if row['container_id'] == request['container_id'] and row['operation'] == operation
                          and request['engine_before_ns'] <= row['begin_wall_time_ns']
                          and row['result_wall_time_ns'] <= request['engine_after_ns']]
            require(candidates, 'required invocation absent from exact CID/operation/window')
            identifiers = {row['invocation_id'] for row in candidates}
            require(not used.intersection(identifiers), 'invocation reused across requests')
            used.update(identifiers)
        state = 'not_applicable' if operation is None else ('unique' if len(candidates) == 1 else 'ambiguous')
        matches.append({'request': request, 'status': state, 'candidates': candidates,
                        'causal_attribution_certified': False})
    return {'schema_version': 1, 'scope': 'exact_CID_guest_clock_candidates_not_Docker_causal_certification',
            'session_id': journal['session_id'], 'boot_id': journal['boot_id'],
            'journal_sha256': journal['journal_sha256'], 'journal_bytes': journal['journal_bytes'],
            'enrollment_sha256': journal['enrollment_sha256'], 'status_sha256': journal['status_sha256'],
            'invocation_count': journal['invocation_count'], 'matches': matches,
            'unmapped_invocations': [row for row in references if row['invocation_id'] not in used],
            'all_requested_relations_unique': all(row['status'] != 'ambiguous' for row in matches),
            'request_receipts_authenticated': False, 'engine_clock_binding_certified': False,
            'docker_operation_mapping_certified': False, 'full_process_absence_certified': False,
            'payload_exit_status_certified': False, 'historical_generation_identity_certified': False}
