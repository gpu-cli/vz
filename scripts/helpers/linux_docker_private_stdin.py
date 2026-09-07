"""Bounded private stdin with exact public acknowledgments, not Docker evidence.

The caller registers Capture and persists its public receipt BEFORE run(). The
frozen pipe transport performs no file writes: its private plan and raw output
exist only in memory. Neither that plan, its hashes, arbitrary child output nor
exception messages are exported. Unexpected output is withheld wholesale, so
encoded secrets are not made safe merely by attempting redaction.

Public argv/environment/acknowledgments must be source-selected by the caller.
Common secret encodings there are rejected as an additional guard, not a claim
to identify every possible covert encoding. No secure-memory-erasure, registry
authentication, Machine ownership or independent secret-byte replay is claimed.
Cancellation is projected as an uncertain result, not re-raised with private
exception state. If the underlying transport raises without returning its child
handle, ownership remains unresolved; this wrapper cannot manufacture a reap.
"""
import base64
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
from urllib.parse import quote_from_bytes

import linux_docker_interactive_capture as transport

MAX_INPUT = 65536
MAX_OUTPUT = 65536
MAX_PUBLIC = 65536
MAX_ARG = 16384


class PrivateStdinError(ValueError):
    """Only source-fixed diagnostics; no child/OS exception text."""


def require(condition, code):
    if not condition:
        raise PrivateStdinError(code)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def _digest(value):
    return hashlib.sha256(value).hexdigest()


def _variants(private):
    variants = set()
    for value in (private, private.rstrip(b'\r\n')):
        if not value:
            continue
        variants.update((value, base64.b64encode(value), base64.urlsafe_b64encode(value),
                         value.hex().encode(), value.hex().upper().encode(),
                         quote_from_bytes(value, safe='').encode()))
        try:
            text = value.decode('utf-8')
        except UnicodeDecodeError:
            continue
        variants.add(json.dumps(text, ensure_ascii=True)[1:-1].encode())
        variants.add(json.dumps(text, ensure_ascii=False)[1:-1].encode())
    return variants


def _stamp(value):
    return type(value) is dict and set(value) == {'unix_ns', 'monotonic_ns'} and all(
        type(value[key]) is int and value[key] > 0 for key in value)


class Result:
    __slots__ = ('stdout', 'stderr', 'returncode', 'receipt', 'pending_process')

    def __init__(self, stdout, stderr, returncode, receipt, pending_process):
        self.stdout, self.stderr, self.returncode = stdout, stderr, returncode
        self.receipt, self.pending_process = copy.deepcopy(receipt), pending_process

    def __repr__(self):
        return '<PrivateStdinResult>'


class Capture:
    """One-shot registered owner; errors never reset its sticky uncertainty."""
    __slots__ = ('_argv', '_executable', '_cwd', '_env', '_private_input', '_expected_stdout',
                 '_expected_stderr', '_expected_exit', '_timeout', '_limit', '_attempted',
                 '_receipt', 'pending_process')

    def __init__(self, argv, *, executable, cwd, env, private_input, expected_stdout,
                 expected_stderr, expected_exit=0, timeout_seconds=30, output_limit=MAX_OUTPUT):
        require(type(private_input) is bytes and 0 < len(private_input) <= MAX_INPUT, 'private_input_bounds')
        require(type(argv) is list and 0 < len(argv) <= 64 and
                all(type(value) is str and value and '\0' not in value and len(value) <= MAX_ARG for value in argv),
                'public_argv_shape')
        require(type(env) is dict and len(env) <= 64 and all(type(key) is str and type(value) is str and
                re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', key) and '\0' not in value and len(value) <= 4096
                for key, value in env.items()), 'public_environment_shape')
        require(type(executable) in (str, Path) or isinstance(executable, Path), 'executable_path_shape')
        require(type(cwd) in (str, Path) or isinstance(cwd, Path), 'cwd_path_shape')
        executable, cwd = os.fspath(executable), os.fspath(cwd)
        require(all(os.path.isabs(value) and '\0' not in value and len(value) <= 4096
                    for value in (executable, cwd)), 'absolute_process_paths')
        require(type(expected_exit) is int and 0 <= expected_exit <= 255, 'expected_exit_shape')
        require(type(timeout_seconds) in (int, float) and math.isfinite(timeout_seconds) and
                0 < timeout_seconds <= 120, 'private_stdin_deadline')
        require(type(output_limit) is int and 0 < output_limit <= MAX_OUTPUT, 'public_output_bound')
        require(all(type(value) is bytes and len(value) <= output_limit
                    for value in (expected_stdout, expected_stderr)), 'public_acknowledgment_shape')
        public = _json({'argv': argv, 'executable': executable, 'cwd': cwd, 'environment': env})
        require(len(public) <= MAX_PUBLIC, 'public_metadata_bound')
        # Scan individual values too: JSON escaping must not hide private bytes.
        try:
            fields = [public, expected_stdout, expected_stderr,
                      *(value.encode() for value in (*argv, executable, cwd, *env.keys(), *env.values()))]
        except UnicodeError:
            raise PrivateStdinError('public_contract_encoding') from None
        require(not any(variant in field for variant in _variants(private_input) for field in fields),
                'private_value_in_public_contract')
        self._argv, self._executable, self._cwd, self._env = list(argv), executable, cwd, dict(env)
        self._private_input = private_input
        self._expected_stdout, self._expected_stderr = expected_stdout, expected_stderr
        self._expected_exit, self._timeout, self._limit = expected_exit, timeout_seconds, output_limit
        self._attempted, self.pending_process = False, None
        self._receipt = {'schema_version': 1, 'scope': 'private_stdin_transport_not_registry_or_Machine_certification',
            'argv': list(argv), 'executable': executable, 'cwd': cwd, 'environment': dict(env),
            'timeout_seconds': timeout_seconds, 'output_limit_each': output_limit, 'input_limit': MAX_INPUT,
            'private_input_hash_published': False, 'private_plan_published': False,
            'expected_exit': expected_exit, 'expected_stdout_sha256': _digest(expected_stdout),
            'expected_stderr_sha256': _digest(expected_stderr), 'effects_uncertain': True,
            'capture_complete': False, 'acknowledged': False, 'owned_process_reaped': False,
            'started': None, 'completed': None, 'pid': None, 'returncode': None,
            'stdin_write_complete': False, 'stdin_eof_count': 0, 'error': None,
            'unexpected_output_withheld': False, 'recovery_attempted': False,
            'pending_process_retained': False, 'process_ownership_unresolved': True}

    @property
    def receipt(self):
        return copy.deepcopy(self._receipt)

    def __repr__(self):
        return '<PrivateStdinCapture>'

    def run(self):
        require(not self._attempted, 'private_stdin_already_attempted')
        self._attempted = True
        self._receipt['started'] = transport.stamp()
        plan = {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': self._timeout,
            'input_limit': len(self._private_input), 'output_limit': self._limit,
            'actions': [{'kind': 'write', 'data': self._private_input}, {'kind': 'close_stdin'}]}
        try:
            captured = transport.capture(list(self._argv), executable=self._executable, cwd=self._cwd,
                                         env=dict(self._env), plan=plan)
            self.pending_process = captured.pending_process
        except BaseException as error:
            # Do not attach/re-raise the exception: it may contain private bytes
            # in text, attributes, stdout, a plan, or an exception cause.
            # Preserve a transport's explicit pending handle if it provides
            # one. Otherwise ownership remains unknown, never certified gone.
            try:
                self.pending_process = getattr(error, 'pending_process', None)
            except BaseException:
                self.pending_process = None
            self._receipt.update(error='transport_exception', unexpected_output_withheld=True,
                                pending_process_retained=self.pending_process is not None,
                                completed=transport.stamp())
            return Result(b'', b'', None, self._receipt, self.pending_process)
        self._receipt['pending_process_retained'] = self.pending_process is not None
        good = False
        try:
            row = captured.receipt
            self._receipt['returncode'] = captured.returncode if type(captured.returncode) is int else None
            self._receipt['pid'] = row.get('pid') if type(row.get('pid')) is int and row['pid'] > 0 else None
            self._receipt['owned_process_reaped'] = row.get('owned_process_reaped') is True and self.pending_process is None
            self._receipt['process_ownership_unresolved'] = not self._receipt['owned_process_reaped']
            self._receipt['recovery_attempted'] = row.get('termination') is not None
            actions = row.get('actions')
            require(type(actions) is list and len(actions) == 2, 'private_actions_incomplete')
            write, eof = actions
            require(type(write.get('index')) is int and write['index'] == 0 and write.get('kind') == 'write' and
                    write.get('trigger') == {'kind': 'immediate'} and write.get('complete') is True and
                    type(write.get('written_bytes')) is int and write['written_bytes'] == len(self._private_input) and
                    type(write.get('input_size')) is int and write['input_size'] == len(self._private_input) and
                    write.get('input_sha256') == _digest(self._private_input),
                    'private_write_incomplete')
            self._receipt['stdin_write_complete'] = True
            require(type(eof.get('index')) is int and eof['index'] == 1 and eof.get('kind') == 'close_stdin' and
                    eof.get('trigger') == {'kind': 'immediate'} and eof.get('complete') is True and
                    type(row.get('stdin_eof_count')) is int and row['stdin_eof_count'] == 1, 'private_eof_incomplete')
            self._receipt['stdin_eof_count'] = 1
            clocks = [row['started'], write['triggered'], write['completed'], eof['triggered'], eof['completed'], row['completed']]
            require(all(_stamp(value) for value in clocks) and all(
                left['monotonic_ns'] <= right['monotonic_ns'] for left, right in zip(clocks, clocks[1:])),
                'private_capture_clock_order')
            require(row['argv'] == self._argv and row['environment'] == self._env and row['executable'] == self._executable and
                    row['cwd'] == self._cwd and row['mode'] == 'pipes' and row['merged_tty'] is False and
                    type(row['input_limit']) is int and row['input_limit'] == len(self._private_input) and
                    type(row['output_limit_each']) is int and row['output_limit_each'] == self._limit and
                    type(row['timeout_seconds']) in (int, float) and row['timeout_seconds'] == self._timeout and
                    type(row['planned_action_count']) is int and row['planned_action_count'] == 2 and
                    row.get('terminal') is None and row['owned_direct_child'] is True and self._receipt['pid'] is not None and
                    row['process_group'] == self._receipt['pid'] == row['session_id'], 'private_capture_identity')
            require(row['capture_complete'] is True and row['effects_uncertain'] is False and
                    self._receipt['owned_process_reaped'] and row.get('error') is None and row.get('cleanup_error') is None and
                    row.get('termination') is None and type(row['returncode']) is int and row['returncode'] == captured.returncode and
                    type(captured.returncode) is int and captured.returncode == self._expected_exit,
                    'private_capture_unsuccessful')
            require(captured.stdout == self._expected_stdout and captured.stderr == self._expected_stderr,
                    'private_output_mismatch')
            expected_outputs = {name: {'size': len(value), 'sha256': _digest(value)} for name, value in
                (('stdout', self._expected_stdout), ('stderr', self._expected_stderr), ('tty', b''))}
            require(_json(row['outputs']) == _json(expected_outputs), 'private_output_capture_binding')
            good = True
        except BaseException:
            self._receipt.update(error='private_capture_unproven', unexpected_output_withheld=True)
        self._receipt['completed'] = transport.stamp()
        if good:
            self._receipt.update(capture_complete=True, effects_uncertain=False, acknowledged=True)
        return Result(self._expected_stdout if good else b'', self._expected_stderr if good else b'',
                      self._receipt['returncode'], self._receipt, self.pending_process)


def capture(argv, **kwargs):
    """Convenience only; stateful callers should register Capture before run()."""
    return Capture(argv, **kwargs).run()
