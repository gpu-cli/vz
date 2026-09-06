"""Finite owned CLI I/O; this primitive neither invokes Docker nor grants cleanup.

PTY stdout is a merged raw terminal transcript, never stdout/stderr demultiplexing.
Only a newly allocated terminal is touched. The supplied environment is complete.
Invalid plans raise before spawn; dispatched failures return uncertain receipts.
"""
import copy
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import os
import selectors
import signal
import struct
import subprocess
import termios
import time

MAX_BYTES = 4 * 1024 * 1024
MAX_ACTIONS = 32
CHUNK = 65536
REAP_SECONDS = 5


class CaptureError(ValueError):
    pass


def require(value, code):
    if not value:
        raise CaptureError(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def stamp():
    return {'unix_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}


def validate_plan(plan):
    require(type(plan) is dict and set(plan) == {
        'schema_version', 'mode', 'timeout_seconds', 'input_limit', 'output_limit', 'actions'}, 'plan_fields')
    require(type(plan['schema_version']) is int and plan['schema_version'] == 1, 'plan_schema')
    require(plan['mode'] in ('pipes', 'pty'), 'plan_mode')
    require(type(plan['timeout_seconds']) in (int, float) and 0 < plan['timeout_seconds'] <= 120,
            'plan_deadline')
    for key in ('input_limit', 'output_limit'):
        require(type(plan[key]) is int and 0 < plan[key] <= MAX_BYTES, 'plan_byte_limit')
    require(type(plan['actions']) is list and len(plan['actions']) <= MAX_ACTIONS, 'plan_actions')
    total, eof = 0, False
    for action in plan['actions']:
        require(type(action) is dict and type(action.get('kind')) is str, 'action_shape')
        kind = action['kind']
        fields = {'write': {'data'}, 'close_stdin': set(), 'resize': {'rows', 'cols'},
                  'signal': {'name'}}
        require(kind in fields and set(action) == {'kind'} | fields[kind] | ({'after'} if 'after' in action else set()),
                'action_fields')
        if 'after' in action:
            after = action['after']
            require(type(after) is dict and set(after) == {'stream', 'marker'}, 'trigger_fields')
            require(after['stream'] in (('tty',) if plan['mode'] == 'pty' else ('stdout', 'stderr')) and
                    type(after['marker']) is bytes and 0 < len(after['marker']) <= 4096, 'trigger_value')
        if kind == 'write':
            require(not eof and type(action['data']) is bytes and bool(action['data']), 'write_data')
            total += len(action['data'])
            require(total <= plan['input_limit'], 'input_limit')
        elif kind == 'close_stdin':
            require(plan['mode'] == 'pipes' and not eof, 'stdin_eof_mode_or_duplicate')
            eof = True
        elif kind == 'resize':
            require(plan['mode'] == 'pty' and all(type(action[k]) is int and 1 <= action[k] <= 4096
                    for k in ('rows', 'cols')), 'resize_dimensions')
        else:
            require(action['name'] in ('SIGINT', 'SIGTERM'), 'signal_not_allowed')
    return copy.deepcopy(plan)


def signal_owned(process, owned_pid, number):
    """No poll/reap before identity checks: the unreaped direct child pins PID."""
    require(process.returncode is None, 'child_already_reaped')
    require(process.pid == owned_pid and os.getpgid(owned_pid) == owned_pid and
            os.getsid(owned_pid) == owned_pid, 'child_group_identity_changed')
    os.killpg(owned_pid, number)


def terminate_owned(process, owned_pid, observation=None):
    if process.returncode is not None:
        return
    signal_owned(process, owned_pid, signal.SIGKILL)
    if observation is not None:
        observation['dispatched'] = True
    process.wait(timeout=REAP_SECONDS)


def terminal_attributes(fd):
    value = termios.tcgetattr(fd)
    return value[:6] + [[v if type(v) is int else v[0] for v in value[6]]]


@dataclass
class CaptureResult:
    stdout: bytes
    stderr: bytes
    returncode: object
    receipt: dict
    pending_process: object = None


def capture(argv, *, executable, cwd, env, plan, progress_observer=None):
    plan = validate_plan(plan)
    require(progress_observer is None or callable(progress_observer), 'progress_observer_shape')
    require(type(argv) is list and bool(argv) and all(type(x) is str and '\0' not in x for x in argv),
            'argv_shape')
    require(type(env) is dict and all(type(k) is type(v) is str and k and '=' not in k and
            '\0' not in k + v for k, v in env.items()), 'environment_shape')
    executable, cwd = os.fspath(executable), os.fspath(cwd)
    require(os.path.isabs(executable) and os.path.isabs(cwd), 'absolute_process_paths')
    receipt = {'schema_version': 1, 'mode': plan['mode'], 'argv': list(argv), 'executable': executable,
               'cwd': cwd, 'environment': dict(env), 'started': stamp(), 'actions': [],
               'timeout_seconds': plan['timeout_seconds'], 'input_limit': plan['input_limit'],
               'output_limit_each': plan['output_limit'], 'planned_action_count': len(plan['actions']),
               'merged_tty': plan['mode'] == 'pty', 'capture_complete': False,
               'effects_uncertain': True, 'owned_process_reaped': False,
               'error': None, 'cleanup_error': None, 'stdin_eof_count': 0,
               'terminal': None, 'termination': None}
    if progress_observer is not None:
        receipt['read_progress'] = []
    process = None
    owned_pid = None
    master = slave = None
    original_termios = original_winsize = None
    output = {'stdout': bytearray(), 'stderr': bytearray(), 'tty': bytearray()}
    selector = selectors.DefaultSelector()
    action_index, active, written = 0, None, 0
    input_fd = None
    deadline = time.monotonic() + plan['timeout_seconds']
    try:
        if plan['mode'] == 'pty':
            master, slave = os.openpty()
            original_termios = termios.tcgetattr(master)
            original_winsize = fcntl.ioctl(master, termios.TIOCGWINSZ, b'\0' * 8)
            receipt['terminal'] = {'initial_attributes': terminal_attributes(master),
                'initial_size': [24, 80], 'controlling_terminal_claimed': False,
                'client_restored_attributes': False, 'restored_verified': False,
                'repaired_by_harness': False}
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
            process = subprocess.Popen(argv, executable=executable, cwd=cwd, env=dict(env),
                stdin=slave, stdout=slave, stderr=slave, start_new_session=True, close_fds=True)
            owned_pid = process.pid
            os.close(slave); slave = None
            input_fd = master
            os.set_blocking(master, False)
            selector.register(master, selectors.EVENT_READ, 'tty')
        else:
            process = subprocess.Popen(argv, executable=executable, cwd=cwd, env=dict(env),
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                start_new_session=True, close_fds=True)
            owned_pid = process.pid
            input_fd = process.stdin.fileno()
            os.set_blocking(input_fd, False)
            for stream, name in ((process.stdout, 'stdout'), (process.stderr, 'stderr')):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream.fileno(), selectors.EVENT_READ, name)
        receipt.update(pid=owned_pid, process_group=owned_pid, session_id=owned_pid,
                       owned_direct_child=True)
        while True:
            require(time.monotonic() < deadline, 'deadline')
            if active is None and action_index < len(plan['actions']):
                action = plan['actions'][action_index]
                trigger = None
                if 'after' not in action:
                    trigger = {'kind': 'immediate'}
                else:
                    after = action['after']
                    offset = output[after['stream']].find(after['marker'])
                    if offset >= 0:
                        trigger = {'kind': 'marker', 'stream': after['stream'], 'start_offset': offset,
                                   'end_offset': offset + len(after['marker']),
                                   'marker_sha256': digest(after['marker'])}
                if trigger is not None:
                    active = {'index': action_index, 'kind': action['kind'], 'trigger': trigger,
                              'triggered': stamp(), 'observed_bytes': {k: len(v) for k, v in output.items()},
                              'complete': False}
                    receipt['actions'].append(active)
                    written = 0
                    if action['kind'] == 'write':
                        active.update(input_size=len(action['data']), input_sha256=digest(action['data']),
                                      written_bytes=0)
            if active is not None:
                action = plan['actions'][action_index]
                kind = action['kind']
                complete = True
                if kind == 'write':
                    try:
                        size = os.write(input_fd, action['data'][written:written + CHUNK])
                        require(size > 0, 'zero_write')
                        written += size
                        active['written_bytes'] = written
                    except BlockingIOError:
                        pass
                    complete = written == len(action['data'])
                elif kind == 'close_stdin':
                    process.stdin.close(); input_fd = None
                    receipt['stdin_eof_count'] += 1
                elif kind == 'resize':
                    fcntl.ioctl(master, termios.TIOCSWINSZ,
                                struct.pack('HHHH', action['rows'], action['cols'], 0, 0))
                    require(struct.unpack('HHHH', fcntl.ioctl(master, termios.TIOCGWINSZ, b'\0' * 8))[:2] ==
                            (action['rows'], action['cols']), 'resize_not_observed')
                    signal_owned(process, owned_pid, signal.SIGWINCH)
                    active.update(rows=action['rows'], cols=action['cols'], signal='SIGWINCH',
                                  signal_scope='owned_cli_group')
                else:
                    signal_owned(process, owned_pid, getattr(signal, action['name']))
                    active.update(signal=action['name'], signal_scope='owned_cli_group')
                if complete:
                    active.update(complete=True, completed=stamp())
                    active = None
                    action_index += 1
            if not selector.get_map():
                process.wait(timeout=max(.001, deadline - time.monotonic()))
                require(action_index == len(plan['actions']), 'output_closed_before_actions_complete')
                break
            for key, _ in selector.select(min(.01, max(0, deadline - time.monotonic()))):
                try:
                    chunk = os.read(key.fd, CHUNK)
                except BlockingIOError:
                    continue
                except OSError as error:
                    if plan['mode'] == 'pty' and error.errno == errno.EIO:
                        chunk = b''
                    else:
                        raise
                if not chunk:
                    selector.unregister(key.fd)
                else:
                    room = plan['output_limit'] - len(output[key.data])
                    output[key.data].extend(chunk[:room])
                    require(len(chunk) <= room, 'output_limit')
                    if progress_observer is not None:
                        require(len(receipt['read_progress']) < 2048, 'progress_observation_limit')
                        observation = {'index': len(receipt['read_progress']), 'stream': key.data,
                            'observed_bytes': {name: len(data) for name, data in output.items()},
                            'observed': stamp()}
                        receipt['read_progress'].append(observation)
                        # Only public counts/clocks reach the source-owned hook;
                        # no bytes and no mutable reference to retained evidence.
                        progress_observer({**observation, 'observed_bytes': dict(observation['observed_bytes']),
                                           'observed': dict(observation['observed'])})
        receipt.update(capture_complete=True, effects_uncertain=False, owned_process_reaped=True)
    except BaseException as error:
        receipt['error'] = str(error) if isinstance(error, CaptureError) else type(error).__name__
        if process is not None:
            try:
                receipt['termination'] = {'signal': 'SIGKILL', 'scope': 'owned_cli_group',
                                          'requested': stamp(), 'dispatched': False}
                if process.returncode is None:
                    terminate_owned(process, owned_pid, receipt['termination'])
            except BaseException as cleanup_error:
                receipt['cleanup_error'] = type(cleanup_error).__name__
    finally:
        selector.close()
        if master is not None:
            try:
                observed = terminal_attributes(master)
                reaped = process is not None and process.returncode is not None
                receipt['terminal']['child_exited_termios'] = observed if reaped else None
                receipt['terminal']['before_harness_restore_attributes'] = observed
                receipt['terminal']['client_restored_attributes'] = (
                    reaped and observed == receipt['terminal']['initial_attributes'])
                receipt['terminal']['repaired_by_harness'] = observed != receipt['terminal']['initial_attributes']
                if not receipt['terminal']['client_restored_attributes']:
                    receipt.update(capture_complete=False, effects_uncertain=True)
                    if receipt['error'] is None:
                        receipt['error'] = 'child_terminal_not_restored'
                termios.tcsetattr(master, termios.TCSANOW, original_termios)
                receipt['terminal']['restore_attempts'] = 1
                fcntl.ioctl(master, termios.TIOCSWINSZ, original_winsize)
                receipt['terminal']['restored_attributes'] = terminal_attributes(master)
                receipt['terminal']['restored_size'] = list(struct.unpack('HHHH',
                    fcntl.ioctl(master, termios.TIOCGWINSZ, b'\0' * 8))[:2])
                receipt['terminal']['restored_verified'] = (
                    receipt['terminal']['restored_attributes'] == receipt['terminal']['initial_attributes'] and
                    fcntl.ioctl(master, termios.TIOCGWINSZ, b'\0' * 8) == original_winsize)
                require(receipt['terminal']['restored_verified'], 'terminal_restore_failed')
            except BaseException as error:
                receipt.update(capture_complete=False, effects_uncertain=True,
                               cleanup_error=type(error).__name__)
            os.close(master)
        if slave is not None:
            os.close(slave)
        if process is not None:
            for pipe in (process.stdin, process.stdout, process.stderr):
                if pipe is not None:
                    pipe.close()
            receipt['owned_process_reaped'] = process.returncode is not None
        receipt.update(completed=stamp(), returncode=process.returncode if process is not None else None,
            outputs={k: {'size': len(v), 'sha256': digest(v)} for k, v in output.items()})
    return CaptureResult(bytes(output['tty'] if plan['mode'] == 'pty' else output['stdout']),
                         bytes(output['stderr']), process.returncode if process is not None else None,
                         receipt, process if process is not None and process.returncode is None else None)
