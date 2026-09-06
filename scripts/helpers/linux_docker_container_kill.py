"""Attached Docker run exit137, caused only by an externally owned KILL.

The parent must authenticate the newly running exact-name container, issue
Docker KILL by full CID, and prove wait/inspect137. This module proves the
attached CLI outcome and live read ordering, not that external authority alone.
"""
import copy
from pathlib import Path
import re
import threading
import time

from docker_host_driver import Driver, regular
import linux_docker_container_fixture as fixture
from linux_docker_container_commands import validate_guard
from linux_docker_container_follow import replay_progress
import linux_docker_interactive_evidence as interactive

LIMIT = 4 * 1024 * 1024
TIMEOUT = 30
ACK = 'source-selected attached run137 capture and external KILL ordering verified'


def require(value, reason):
    if not value:
        raise ValueError(reason)


def specification(name, image_id, token):
    fixture.token(token)
    require(name == token + '-sigkill', 'exact owned signal-test name required')
    require(type(image_id) is str and re.fullmatch(r'sha256:[0-9a-f]{64}', image_id), 'immutable image required')
    args = ['run', '--pull', 'never', '--network', 'none', '--restart', 'no', '--name', name,
            '--label', 'dev.vz.container-io=' + token, image_id, 'service', token]
    plan = {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': TIMEOUT,
            'input_limit': 1, 'output_limit': LIMIT, 'actions': [{'kind': 'close_stdin'}]}
    ready = {stream: fixture.encode({'schema_version': 1, 'type': 'service_ready', 'token': token,
        'pid': 1, 'health': 'starting', 'output': stream}) + b'\n' for stream in ('stdout', 'stderr')}
    return args, plan, ready


def external_termination(termination):
    require(type(termination) is dict and set(termination) == {'cid', 'command_index', 'started_unix_ns'} and
            type(termination['cid']) is str and re.fullmatch(r'[0-9a-f]{64}', termination['cid']) and
            all(type(termination[k]) is int and termination[k] > 0 for k in ('command_index','started_unix_ns')),
            'exact externally authenticated KILL binding required')
    return copy.deepcopy(termination)


def rebuild(output, inputs, name, image_id, token, *, environment, termination):
    """Validate semantics before any negative mutation acknowledgement."""
    output = Path(output)
    args, plan, ready = specification(name, image_id, token)
    termination = external_termination(termination)
    capture = interactive.validate_recorded(output, 3,
        argv=['docker', '--config', inputs['docker_config'], '--context', inputs['scope']['docker_context'], *args],
        executable=inputs['clients']['docker']['path'], env=environment, expected_exit=137, expected_plan=plan)
    stdout, stderr = [regular(output / ('command-00003.' + stream), LIMIT) for stream in ('stdout','stderr')]
    require(stdout == ready['stdout'] and stderr == ready['stderr'], 'exact ready-only run streams required')
    semantic = fixture.validate_service(stdout, stderr, token)
    terminal = interactive.parse(regular(output / 'command-00003.json', 8 * 1024 * 1024))
    observed = replay_progress(terminal['interaction_capture'], stdout, stderr, ready, termination['started_unix_ns'])
    retained = interactive.parse(regular(output / 'kill-ready.json', 65536))
    require(interactive.canonical(retained) == interactive.canonical({'schema_version': 1,
            'command_index': 3, 'read_observation': observed}), 'durable live readiness differs')
    first, last = validate_guard(output, inputs, 1, 2), validate_guard(output, inputs, 4, 5)
    before = first['commands'][-1]['receipt']; after = last['commands'][0]['receipt']
    require(before['started_unix_ns'] + before['elapsed_ns'] <= terminal['started_unix_ns'] and
            terminal['started_unix_ns'] + terminal['elapsed_ns'] <= after['started_unix_ns'],
            'attached run escaped observer guards')
    indices = sorted(int(p.name[8:-5]) for p in output.glob('command-*.json')
                     if re.fullmatch(r'command-[0-9]{5}\.json', p.name))
    intents = sorted(p.name for p in output.glob('command-*.intent.json'))
    require(indices == [1,2,3,4,5] and intents == ['command-%05d.intent.json' % i for i in indices],
            'exact five-command observer ledger required')
    return {'schema_version': 1, 'scope': 'attached_run137_external_CID_KILL_wait_inspect_proof_required',
            'name': name, 'image_id': image_id, 'container_id': termination['cid'], 'token': token,
            'command_index': 3, 'capture': capture, 'semantic': semantic,
            'ready_observation': observed, 'termination': termination,
            'guard_terminal_sha256': [v['terminal_sha256'] for g in (first,last) for v in g['commands']]}


def replay_kill(output, inputs, name, image_id, token, proof, *, environment, termination):
    fixture.fixture_contract()
    rebuilt = rebuild(output, inputs, name, image_id, token, environment=environment, termination=termination)
    ack = interactive.parse(regular(Path(output) / 'command-00003.acknowledgement.json', 65536))
    require(interactive.canonical(ack) == interactive.canonical({'command_index': 3, 'assertion': ACK,
            'terminal_receipt_sha256': rebuilt['capture']['terminal_receipt_sha256'], 'effects_uncertain': False}),
            'run137 acknowledgement differs')
    disposition = interactive.parse(regular(Path(output) / 'kill-disposition.json', 65536))
    require(interactive.canonical(disposition) == interactive.canonical({'schema_version': 1,
            'thread_joined': True, 'capture_error_type': None, 'orchestration_error_type': None,
            'pending_interactions': 0}), 'attached run observer disposition unresolved')
    require(interactive.canonical(rebuilt) == interactive.canonical(proof), 'attached run137 raw proof differs')
    return rebuilt


def run_kill(item, name, image_id, token, *, terminate, register_observer):
    require(callable(terminate) and callable(register_observer), 'owned observer callbacks required')
    fixture.fixture_contract()
    args, plan, prefixes = specification(name, image_id, token)
    observer = Driver(item.inputs, item.fixture, item.output / 'kill-run')
    require(observer.record.max_stream_bytes == LIMIT, 'observer stream bound differs')
    register_observer(observer)
    item.guard(); observer.guard()
    require(observer.record.count == 2, 'fresh observer guard count differs')
    ready, done = threading.Event(), threading.Event()
    state = {}
    observer.follow_state = state
    def progress(row):
        # The core deliberately exposes counts, not arbitrary output bytes.
        # Parent inspect authenticates the running object before KILL; final
        # replay requires these exact prefixes, never count-only acceptance.
        if not ready.is_set() and all(row['observed_bytes'][stream] >= len(prefixes[stream]) for stream in prefixes):
            state['ready'] = copy.deepcopy(row)
            observer.record.persist(observer.output / 'kill-ready.json', {'schema_version': 1,
                'command_index': 3, 'read_observation': row}, create=True)
            ready.set()
    def attached():
        try:
            state['command'] = observer.command(args, expected=None, timeout=TIMEOUT,
                                                interaction_plan=plan, progress_observer=progress)
        except BaseException as error:
            state['error'] = error
        finally:
            done.set()
    thread = threading.Thread(target=attached, name='vz-owned-run137', daemon=True)
    observer.follow_thread = thread
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not ready.wait(.05):
            require(not done.is_set(), 'attached run exited before live readiness')
            require(time.monotonic() < deadline, 'attached run readiness deadline')
        require(not done.is_set(), 'attached run exited before external KILL')
        item.guard()
        termination = external_termination(terminate())
        state['termination'] = termination
        thread.join(TIMEOUT + 6)
        require(not thread.is_alive() and 'error' not in state and 'command' in state,
                'attached run completion unproven')
        require(state['command'].index == 3, 'attached observer command index changed')
        observer.guard(); item.guard()
        proof = rebuild(observer.output, item.inputs.raw, name, image_id, token,
                        environment=observer.env, termination=termination)
        observer.record.acknowledge_negative(state['command'], ACK)
    except BaseException as error:
        state['orchestration_error'] = error
        raise
    finally:
        thread.join(TIMEOUT + 6)
        observer.record.persist(observer.output / 'kill-disposition.json', {'schema_version': 1,
            'thread_joined': not thread.is_alive(),
            'capture_error_type': type(state['error']).__name__ if 'error' in state else None,
            'orchestration_error_type': type(state['orchestration_error']).__name__
                if 'orchestration_error' in state else None,
            'pending_interactions': len(observer.record.pending_interactions)}, create=True)
    proof = replay_kill(observer.output, item.inputs.raw, name, image_id, token, proof,
                        environment=observer.env, termination=termination)
    observer.record.persist(observer.output / 'kill-proof.json', proof, create=True)
    return proof
