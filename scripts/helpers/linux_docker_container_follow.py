"""Bounded live log following with pre-TERM read evidence, not lifecycle proof."""
import copy
from pathlib import Path
import re
import threading
import time

from docker_host_driver import Driver, regular
import linux_docker_container_fixture as fixture
import linux_docker_interactive_evidence as interactive

LIMIT = 4 * 1024 * 1024
TIMEOUT = 30


def require(value, reason):
    if not value:
        raise ValueError(reason)


def specification(cid, token):
    require(type(cid) is str and re.fullmatch(r'[0-9a-f]{64}', cid), 'exact follow container ID required')
    fixture.token(token)
    plan = {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': TIMEOUT,
            'input_limit': 1, 'output_limit': LIMIT, 'actions': [{'kind': 'close_stdin'}]}
    ready = {}
    for stream in ('stdout', 'stderr'):
        ready[stream] = fixture.encode({'schema_version': 1, 'type': 'service_ready', 'token': token,
            'pid': 1, 'health': 'starting', 'output': stream}) + b'\n'
    return ['logs', '--follow', cid], plan, ready


def replay_progress(capture, stdout, stderr, ready, termination_started_unix_ns):
    require(type(termination_started_unix_ns) is int and termination_started_unix_ns > 0,
            'external TERM timestamp required')
    for name, raw in (('stdout', stdout), ('stderr', stderr)):
        require(raw.startswith(ready[name]), 'follow initial public stream differs')
    rows = capture.get('read_progress')
    require(type(rows) is list and 1 <= len(rows) <= 2048, 'bounded read observations required')
    counts = {'stdout': 0, 'stderr': 0, 'tty': 0}
    bounds = {'stdout': len(stdout), 'stderr': len(stderr), 'tty': 0}
    start, end = capture['started'], capture['completed']
    previous, previous_wall = start['monotonic_ns'], start['unix_ns']
    first = None
    for index, row in enumerate(rows):
        require(type(row) is dict and set(row) == {'index', 'stream', 'observed_bytes', 'observed'} and
                type(row['index']) is int and row['index'] == index and row['stream'] in ('stdout', 'stderr'),
                'read observation identity differs')
        actual, stamp = row['observed_bytes'], row['observed']
        mono = interactive.timestamp(stamp)
        wall = stamp['unix_ns']
        require(previous <= mono <= end['monotonic_ns'] and previous_wall <= wall <= end['unix_ns'] and
                abs((wall-start['unix_ns'])-(mono-start['monotonic_ns'])) <= interactive.CLOCK_TOLERANCE_NS,
                'read observation clock differs')
        require(type(actual) is dict and set(actual) == set(counts) and all(type(actual[name]) is int and
                counts[name] <= actual[name] <= bounds[name] for name in counts) and
                actual[row['stream']] > counts[row['stream']] and all(actual[name] == counts[name]
                for name in counts if name != row['stream']), 'read prefix counts differ')
        counts, previous, previous_wall = actual, mono, wall
        if first is None and all(counts[name] >= len(ready[name]) for name in ready):
            first = row
    require(counts == bounds and first is not None, 'incomplete followed stream observations')
    require(first['observed']['unix_ns'] < termination_started_unix_ns <= end['unix_ns'],
            'TERM not after both live ready streams')
    return copy.deepcopy(first)


def replay_follow(output, inputs, cid, token, proof, *, environment, termination_started_unix_ns):
    fixture.fixture_contract()
    args, plan, ready = specification(cid, token)
    require(type(proof) is dict and type(proof.get('command_index')) is int, 'follow proof shape differs')
    index = proof['command_index']
    argv = ['docker', '--config', inputs['docker_config'], '--context', inputs['scope']['docker_context'], *args]
    capture_proof = interactive.validate_recorded(output, index, argv=argv,
        executable=inputs['clients']['docker']['path'], env=environment, expected_exit=0, expected_plan=plan)
    stem = 'command-%05d' % index
    stdout, stderr = [regular(Path(output) / (stem+'.'+name), LIMIT) for name in ('stdout', 'stderr')]
    receipt = interactive.parse(regular(Path(output)/(stem+'.json'), 8*1024*1024))
    semantic = fixture.validate_service(stdout, stderr, token, signals=('SIGTERM',), exit_code=143)
    ready_observed = replay_progress(receipt['interaction_capture'], stdout, stderr, ready,
                                     termination_started_unix_ns)
    retained = interactive.parse(regular(Path(output)/'follow-ready.json', 65536))
    require(interactive.canonical(retained) == interactive.canonical({'schema_version': 1,
            'command_index': index, 'read_observation': ready_observed}), 'durable live readiness differs')
    rebuilt = {'schema_version': 1, 'scope': 'live_follow_only_external_TERM_and_service_lifecycle_proof_required',
        'container_id': cid, 'token': token, 'command_index': index, 'capture': capture_proof,
        'semantic': semantic, 'ready_observation': ready_observed,
        'termination_started_unix_ns': termination_started_unix_ns}
    require(interactive.canonical(rebuilt) == interactive.canonical(proof), 'follow raw proof differs')
    return rebuilt


def run_follow(item, cid, token, *, service_guard, terminate, register_follower):
    require(all(callable(value) for value in (service_guard, terminate, register_follower)),
            'source-owned follow callbacks required')
    fixture.fixture_contract()
    args, plan, prefixes = specification(cid, token)
    follower = Driver(item.inputs, item.fixture, item.output / 'follow')
    require(follower.record.max_stream_bytes == LIMIT, 'follow output bound differs')
    register_follower(follower)  # Retained by the caller even when dispatch fails.
    item.guard(); service_guard(cid, token); follower.guard()
    index = follower.record.count + 1
    ready, done = threading.Event(), threading.Event()
    state = {}
    follower.follow_state = state
    def progress(row):
        if not ready.is_set() and all(row['observed_bytes'][name] >= len(prefixes[name]) for name in prefixes):
            state['ready'] = copy.deepcopy(row)
            follower.record.persist(follower.output/'follow-ready.json', {'schema_version': 1,
                'command_index': index, 'read_observation': row}, create=True)
            ready.set()
    def follow():
        try:
            state['command'] = follower.command(args, expected=0, timeout=TIMEOUT,
                                                interaction_plan=plan, progress_observer=progress)
        except BaseException as error:
            state['error'] = error
        finally:
            done.set()
    thread = threading.Thread(target=follow, name='vz-owned-log-follow', daemon=True)
    follower.follow_thread = thread
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not ready.wait(.05):
            require(not done.is_set(), 'log follower exited before live readiness')
            require(time.monotonic() < deadline, 'log follower readiness deadline')
        require(not done.is_set(), 'log follower exited before TERM')
        item.guard(); service_guard(cid, token)
        termination = terminate()
        require(type(termination) is dict and type(termination.get('command_index')) is int and
                termination['command_index'] > 0 and type(termination.get('started_unix_ns')) is int,
                'external TERM command binding absent')
        state['termination'] = termination
        thread.join(TIMEOUT + 6)
        require(not thread.is_alive() and 'error' not in state and 'command' in state,
                'log follower completion unproven')
        command = state['command']
        require(command.index == index, 'concurrent follower command index changed')
        follower.guard(); item.guard()
        capture_proof = interactive.validate_recorded(follower.output, index,
            argv=['docker', '--config', item.inputs.raw['docker_config'], '--context',
                  item.inputs.scope['docker_context'], *args],
            executable=item.inputs.raw['clients']['docker']['path'], env=follower.env,
            expected_exit=0, expected_plan=plan)
        semantic = fixture.validate_service(command.stdout, command.stderr, token,
                                            signals=('SIGTERM',), exit_code=143)
        row = {'schema_version': 1, 'scope': 'live_follow_only_external_TERM_and_service_lifecycle_proof_required',
            'container_id': cid, 'token': token, 'command_index': index, 'capture': capture_proof,
            'semantic': semantic, 'ready_observation': state['ready'],
            'termination_started_unix_ns': termination['started_unix_ns']}
        return replay_follow(follower.output, item.inputs.raw, cid, token, row, environment=follower.env,
                             termination_started_unix_ns=termination['started_unix_ns'])
    except BaseException as error:
        state['orchestration_error'] = error
        raise
    finally:
        # Never relaunch or retry TERM. Core capture owns its bounded child
        # disposition; retain this thread/Driver if that disposition is unknown.
        thread.join(TIMEOUT + 6)
        follower.record.persist(follower.output/'follow-disposition.json', {
            'schema_version': 1, 'thread_joined': not thread.is_alive(),
            'capture_error_type': type(state['error']).__name__ if 'error' in state else None,
            'orchestration_error_type': type(state['orchestration_error']).__name__
                if 'orchestration_error' in state else None,
            'pending_interactions': len(follower.record.pending_interactions)}, create=True)
