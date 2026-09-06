"""Separate exact-Machine Exec ledger and no-dispatch process-observation replay.

The caller brackets every capture with authenticated Docker guards and inspect
records. This module does not establish historical runtime invocation or absence
of processes that were never sampled. Replay cannot dispatch or mutate receipts.
"""
from pathlib import Path
import re

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_buildkit_cgroup as binding_module
import linux_docker_container_process as process
import linux_docker_interactive_evidence as evidence

require = driver.require
MAX_COMMANDS = 128
TIMEOUT = 40
MAX_ELAPSED_NS = 60 * 1_000_000_000
SCOPE = 'DEV_exact_Machine_process_observation_not_historical_invocation_proof'


def clone(value):
    return evidence.parse(evidence.canonical(value))


def same(left, right):
    # JSON booleans are not integer counters/schema versions.
    return evidence.canonical(left) == evidence.canonical(right)


def required_source_paths():
    return [str(Path(module.__file__).resolve(strict=True)) for module in
            (process, startup, driver, binding_module, evidence)] + [str(Path(__file__).resolve(strict=True))]


def read(path):
    return startup.read_private_regular(path, startup.LIMIT)


def document(path):
    return evidence.parse(read(path))


def source_inputs(harness, descriptor, source_pins):
    cli = str(startup.canonical(str(harness.cli)))
    require(isinstance(source_pins, dict) and set(source_pins) == set(required_source_paths()) | {cli},
            'exact process observation source and staged CLI pins required')
    for path, digest in source_pins.items():
        require(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest) and
                str(startup.canonical(path)) == path and startup.digest(Path(path)) == digest,
                'process observation source or CLI digest changed')
    owner = descriptor.get('owner')
    require(isinstance(owner, dict) and all(isinstance(owner.get(key), str) and
            re.fullmatch('[A-Za-z0-9_-]{1,128}', owner[key]) for key in
            ('project_id', 'environment_id', 'machine_id')), 'exact process observation owner required')
    require(isinstance(harness.env, dict) and all(isinstance(key, str) and isinstance(value, str)
            for key, value in harness.env.items()), 'exact public Exec environment required')
    return clone({'schema_version': 1, 'scope': SCOPE, 'cli': cli,
                  'source_pins': source_pins, 'environment': harness.env, 'descriptor': descriptor,
                  'project_binding': binding_module.project_binding(harness, descriptor)})


def request(inspected, phase, previous, engine_policy, label, expected_boot_id):
    require(isinstance(label, str) and re.fullmatch('[a-z][a-z0-9-]{0,79}', label),
            'source-selected bounded process observation label required')
    require(phase in ('running', 'stopped', 'removed'), 'unknown process observation phase')
    return clone({'inspected': inspected, 'phase': phase, 'previous': previous,
                  'engine_policy': engine_policy, 'label': label, 'expected_boot_id': expected_boot_id})


def arguments(inputs, selected):
    owner = inputs['descriptor']['owner']
    script = process.probe_script(selected['inspected']['Id'])
    require(isinstance(script, str) and 0 < len(script.encode()) <= startup.LIMIT,
            'bounded source-selected process probe required')
    return [inputs['cli'], 'exec', '--environment', owner['environment_id'], '--machine', owner['machine_id'],
            '--no-stdin', '--timeout', '30', '--', '/bin/busybox', 'sh', '-c', script]


def sampler_previous(output, inputs, index, selected):
    previous = selected['previous']
    if previous is None:
        return None
    require(type(previous) is dict and previous.get('phase') == 'running' and
            type(previous.get('command_index')) is int and 0 < previous['command_index'] < index,
            'previous running process observation from this ledger required')
    descriptor = inputs['descriptor']
    require(previous.get('owner') == descriptor['owner'] and
            previous.get('incarnation_id') == descriptor['incarnation_id'] and
            previous.get('incarnation_generation') == descriptor['incarnation_generation'] and
            previous.get('project_binding') == inputs['project_binding'],
            'previous process observation belongs to a foreign Machine or project')
    old_index = previous['command_index']
    old_request = document(output / ('request-%03d.json' % old_index))
    require(old_request.get('phase') == 'running' and old_request.get('previous') is None,
            'previous process birth must come from an original running observation')
    require(same(document(output / ('proof-%03d.json' % old_index)), previous) and
            same(result_proof(output, inputs, old_index, old_request), previous),
            'previous process observation differs from its raw source-selected receipt')
    return previous['observation']


def result_proof(output, inputs, index, selected):
    """Read complete original streams and reconstruct the exact command proof."""
    require(type(index) is int and 0 < index <= MAX_COMMANDS, 'process command index out of bounds')
    stem = '%03d-%s' % (index, selected['label'])
    intent_raw, result_raw = [read(output / (stem + suffix)) for suffix in ('.intent.json', '.result.json')]
    intent, row = evidence.parse(intent_raw), evidence.parse(result_raw)
    stdout, stderr = [read(output / (stem + suffix)) for suffix in ('.stdout', '.stderr')]
    expected = {'index': index, 'label': selected['label'], 'argv': arguments(inputs, selected),
                'argv0': inputs['cli'], 'executable': inputs['cli'],
                'cwd': inputs['project_binding']['project_path'], 'timeout_seconds': TIMEOUT,
                'effects_uncertain': True, 'capture_complete': False, 'termination_scope': 'observer_pid_only'}
    require(type(intent) is dict and set(intent) == set(expected) | {'started_unix_ns'} and
            same({key: intent[key] for key in expected}, expected) and
            type(intent['started_unix_ns']) is int and intent['started_unix_ns'] > 0,
            'process observation intent differs from source-selected command')
    fields = {'exit_code', 'elapsed_ns', 'error', 'stdout_sha256', 'stderr_sha256',
              'retained_stdout_bytes', 'retained_stderr_bytes', 'hashes_cover'}
    require(type(row) is dict and set(row) == set(intent) | fields and
            same({key: row[key] for key in intent if key not in ('effects_uncertain', 'capture_complete')},
                 {key: value for key, value in intent.items() if key not in ('effects_uncertain', 'capture_complete')}),
            'process observation result differs from original intent')
    require(row['effects_uncertain'] is False and row['capture_complete'] is True and
            type(row['exit_code']) is int and row['exit_code'] == 0 and row['error'] is None and
            row['hashes_cover'] == 'complete_streams' and
            type(row['elapsed_ns']) is int and 0 < row['elapsed_ns'] <= MAX_ELAPSED_NS,
            'incomplete, failed, uncertain or unbounded process observation')
    for name, raw in (('stdout', stdout), ('stderr', stderr)):
        require(row[name + '_sha256'] == driver.sha256(raw) and
                type(row['retained_' + name + '_bytes']) is int and row['retained_' + name + '_bytes'] == len(raw),
                'process observation retained stream digest or length differs')
    require(stderr == b'', 'process observation public Exec diagnostics')
    observation = process.validate(stdout, inspected=selected['inspected'], phase=selected['phase'],
                                   previous=sampler_previous(output, inputs, index, selected),
                                   engine_policy=selected['engine_policy'], expected_boot_id=selected['expected_boot_id'])
    descriptor = inputs['descriptor']
    return {'schema_version': 1, 'scope': SCOPE, 'command_index': index, 'label': selected['label'],
            'phase': selected['phase'], 'started_unix_ns': row['started_unix_ns'],
            'finished_unix_ns': row['started_unix_ns'] + row['elapsed_ns'],
            'owner': clone(descriptor['owner']), 'incarnation_id': descriptor['incarnation_id'],
            'incarnation_generation': descriptor['incarnation_generation'],
            'project_binding': clone(inputs['project_binding']),
            'request_sha256': driver.sha256(evidence.canonical(selected)),
            'receipt_sha256': driver.sha256(result_raw), 'stdout_sha256': driver.sha256(stdout),
            'stderr_sha256': driver.sha256(stderr), 'observation': observation}


class Observer:
    def __init__(self, harness, descriptor, output, source_pins):
        self.harness, self.descriptor = harness, clone(descriptor)
        self.output = Path(output)
        require(self.output.is_absolute() and self.output.parent == self.output.parent.resolve(strict=True),
                'canonical process observer output required')
        self.inputs = source_inputs(harness, self.descriptor, source_pins)
        startup.private(self.output)
        self.record = startup.Recorder(self.output, clone(self.inputs['environment']))
        self.record.canaries = list(harness.record.canaries)
        startup.document(self.output / 'inputs.json', self.inputs)

    def verify_inputs(self):
        require(same(source_inputs(self.harness, self.descriptor, self.inputs['source_pins']), self.inputs) and
                same(document(self.output / 'inputs.json'), self.inputs) and
                self.record.env == self.inputs['environment'], 'process observer original inputs changed')

    def capture(self, inspected, *, phase, previous=None, engine_policy=None, label='process', expected_boot_id=None):
        self.verify_inputs()
        selected = request(inspected, phase, previous, engine_policy, label, expected_boot_id)
        process.policy(engine_policy, inspected)
        index = len(self.record.receipts) + 1
        require(index <= MAX_COMMANDS, 'process observation ledger exceeds bound')
        sampler_previous(self.output, self.inputs, index, selected)
        startup.document(self.output / ('request-%03d.json' % index), selected)
        self.record.run(label, arguments(self.inputs, selected), cwd=Path(self.inputs['project_binding']['project_path']),
                        executable=self.inputs['cli'], timeout=TIMEOUT, observer_only=True)
        self.verify_inputs()
        proof = result_proof(self.output, self.inputs, index, selected)
        startup.document(self.output / ('proof-%03d.json' % index), proof)
        return proof

    def replay(self):
        self.verify_inputs()
        return ReplayObserver(self)


class ReplayObserver:
    """Retained raw bytes only: no Recorder, dispatch, acknowledgement or writes."""
    def __init__(self, live):
        self.live, self.output, self.inputs = live, live.output, clone(live.inputs)
        self.expected_count, self.count = len(live.record.receipts), 0
        self.labels = []

    def capture(self, inspected, *, phase, previous=None, engine_policy=None, label='process', expected_boot_id=None):
        self.live.verify_inputs()
        selected = request(inspected, phase, previous, engine_policy, label, expected_boot_id)
        index = self.count + 1
        require(index <= self.expected_count, 'missing source-selected process observation')
        require(same(document(self.output / ('request-%03d.json' % index)), selected),
                'process observation request differs from source-selected replay')
        proof = result_proof(self.output, self.inputs, index, selected)
        require(same(document(self.output / ('proof-%03d.json' % index)), proof),
                'process observation proof differs from independent raw replay')
        self.live.verify_inputs()
        self.count = index
        self.labels.append(label)
        return proof

    def assert_complete(self):
        self.live.verify_inputs()
        require(self.count == self.expected_count and 0 < self.count <= MAX_COMMANDS,
                'extra, missing or empty process observation ledger')
        names = {'inputs.json'}
        for index, label in enumerate(self.labels, 1):
            names.update('%03d-%s%s' % (index, label, suffix) for suffix in
                         ('.intent.json', '.result.json', '.stdout', '.stderr'))
            names.update('%s-%03d.json' % (kind, index) for kind in ('request', 'proof'))
        require({path.name for path in self.output.iterdir()} == names,
                'unexpected or missing process observation evidence files')
        return {'schema_version': 1, 'scope': SCOPE, 'command_count': self.count}
