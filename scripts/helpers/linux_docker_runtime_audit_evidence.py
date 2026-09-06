"""Exact-Machine audit enrollment, bounded capture, and no-dispatch replay.

Enrollment is a mutation even though the public CLI observer has PID-only
termination. Failed enrollment is never repaired, adopted, retried or silently
cleared. Sessions are separate from workload cleanup: enroll before mutations,
join all observers/remove owned Docker objects, capture, then permit public Stop.
"""
from pathlib import Path

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_buildkit_cgroup as binding
import linux_docker_interactive_evidence as evidence
import linux_docker_runtime_audit as audit
import linux_docker_runtime_audit_capture as probe

require = driver.require
TIMEOUT = 40
MAX_ELAPSED_NS = 60 * 1_000_000_000
SCOPE = 'DEV_Machine_bound_youki_journal_not_Docker_operation_or_full_process_certification'


def clone(value):
    return evidence.parse(evidence.canonical(value))


def same(left, right):
    return evidence.canonical(left) == evidence.canonical(right)


def required_source_paths():
    return [str(Path(module.__file__).resolve(strict=True)) for module in
            (driver, startup, binding, evidence, audit, probe)] + [str(Path(__file__).resolve(strict=True))]


def read(path):
    return startup.read_private_regular(path, startup.LIMIT)


def document(path):
    return evidence.parse(read(path))


def source_inputs(harness, descriptor, source_pins, session_id):
    require(audit.matches(session_id, r'[0-9a-f]{64}'), 'invalid source-selected audit session')
    require(type(harness.env) is dict and all(type(key) is str and type(value) is str
            for key, value in harness.env.items()), 'exact string environment required')
    cli = str(startup.canonical(str(harness.cli)))
    require(type(source_pins) is dict and set(source_pins) == set(required_source_paths()) | {cli},
            'exact audit source and staged CLI pins required')
    for path, digest in source_pins.items():
        require(audit.matches(digest, r'[0-9a-f]{64}') and str(startup.canonical(path)) == path and
                startup.digest(Path(path)) == digest, 'audit source or CLI bytes changed')
    project = binding.project_binding(harness, descriptor)
    owner = descriptor['owner']
    require(all(audit.matches(owner.get(key), r'[A-Za-z0-9_-]{1,128}') for key in
                ('project_id', 'environment_id', 'machine_id')), 'invalid exact audit Machine owner')
    original = startup.canonical(harness.info['developer_bundle'])
    staged = startup.canonical(str(harness.prefix / 'linux/developer'))
    runtime_files = {}
    for filename in ('youki', 'version.json'):
        source_path, staged_path = original / filename, staged / filename
        pinned = harness.info['inputs'][str(source_path)]
        require(startup.digest(source_path) == startup.digest(staged_path) == pinned ==
                harness.staged_inputs[str(staged_path)], 'audit runtime artifact changed after staging')
        runtime_files[str(source_path)] = runtime_files[str(staged_path)] = pinned
    runtime_sha = runtime_files[str(original / 'youki')]
    version = evidence.parse(driver.regular(original / 'version.json'))
    require(version.get('profile') == 'developer' and version.get('sha256_youki') == runtime_sha,
            'audit runtime version metadata differs from pinned binary')
    selected_path = harness.evidence / (owner['machine_id'] + '-runtime-binding.json')
    selected_raw = read(selected_path)
    selected = evidence.parse(selected_raw)
    require(selected.get('owner') == owner and selected['public_status']['docker_context'] == descriptor and
            selected['runtime_evidence']['youki_sha256'] == runtime_sha and
            selected['configuration']['artifact']['youki_sha256'] == runtime_sha and
            selected['configuration']['artifact']['version_sha256'] == runtime_files[str(original / 'version.json')],
            'audit runtime selection differs from authenticated Machine binding')
    return clone({'schema_version': 1, 'scope': SCOPE, 'session_id': session_id, 'cli': cli,
                  'source_pins': source_pins, 'environment': harness.env, 'descriptor': descriptor,
                  'project_binding': project, 'runtime_sha256': runtime_sha, 'runtime_files': runtime_files,
                  'runtime_binding_path': str(selected_path), 'runtime_binding_sha256': driver.sha256(selected_raw)})


def argv(inputs, script):
    require(type(script) is str and 0 < len(script.encode()) <= startup.LIMIT, 'bounded source-selected audit script required')
    owner = inputs['descriptor']['owner']
    return [inputs['cli'], 'exec', '--environment', owner['environment_id'], '--machine', owner['machine_id'],
            '--no-stdin', '--timeout', '30', '--', '/bin/busybox', 'sh', '-c', script]


def command_proof(output, inputs, index, label, script):
    stem = '%03d-%s' % (index, label)
    intent = document(output / (stem + '.intent.json'))
    result_raw = read(output / (stem + '.result.json'))
    row = evidence.parse(result_raw)
    stdout, stderr = [read(output / (stem + '.' + stream)) for stream in ('stdout', 'stderr')]
    expected = {'index': index, 'label': label, 'argv': argv(inputs, script), 'argv0': inputs['cli'],
                'executable': inputs['cli'], 'cwd': inputs['project_binding']['project_path'], 'timeout_seconds': TIMEOUT,
                'effects_uncertain': True, 'capture_complete': False, 'termination_scope': 'observer_pid_only'}
    require(type(intent) is dict and set(intent) == set(expected) | {'started_unix_ns'} and
            same({key: intent[key] for key in expected}, expected) and audit.integer(intent['started_unix_ns']),
            'audit command intent differs from source-selected dispatch')
    final_fields = {'exit_code', 'elapsed_ns', 'error', 'stdout_sha256', 'stderr_sha256',
                    'retained_stdout_bytes', 'retained_stderr_bytes', 'hashes_cover'}
    stable = set(intent) - {'effects_uncertain', 'capture_complete'}
    require(type(row) is dict and set(row) == set(intent) | final_fields and
            same({key: row[key] for key in stable}, {key: intent[key] for key in stable}) and
            row['effects_uncertain'] is False and row['capture_complete'] is True and
            type(row['exit_code']) is int and row['exit_code'] == 0 and row['error'] is None and
            row['hashes_cover'] == 'complete_streams' and audit.integer(row['elapsed_ns'], high=MAX_ELAPSED_NS),
            'audit command incomplete, uncertain, failed or unbounded')
    for name, raw in (('stdout', stdout), ('stderr', stderr)):
        require(row[name + '_sha256'] == driver.sha256(raw) and
                type(row['retained_' + name + '_bytes']) is int and row['retained_' + name + '_bytes'] == len(raw),
                'audit retained stream digest or size differs')
    require(not stderr, 'audit public Exec diagnostics invalidate capture')
    proof = {'command_index': index, 'label': label, 'started_unix_ns': row['started_unix_ns'],
             'finished_unix_ns': row['started_unix_ns'] + row['elapsed_ns'],
             'receipt_sha256': driver.sha256(result_raw), 'stdout_sha256': driver.sha256(stdout),
             'stderr_sha256': driver.sha256(stderr)}
    return stdout, proof


def enrollment_proof(inputs, snapshot, command):
    return {'schema_version': 1, 'scope': SCOPE, 'session_id': inputs['session_id'],
            'owner': clone(inputs['descriptor']['owner']), 'incarnation_id': inputs['descriptor']['incarnation_id'],
            'incarnation_generation': inputs['descriptor']['incarnation_generation'], 'boot_id': snapshot['boot_id'],
            'runtime_sha256': inputs['runtime_sha256'], 'inputs_sha256': driver.sha256(evidence.canonical(inputs)),
            'snapshot': snapshot, 'command': command, 'enrollment_certain': True}


class Session:
    def __init__(self, harness, descriptor, output, source_pins, session_id):
        self.harness, self.descriptor = harness, clone(descriptor)
        self.output = Path(output)
        require(self.output.is_absolute() and self.output.parent == self.output.parent.resolve(strict=True),
                'canonical audit evidence output required')
        self.inputs = source_inputs(harness, self.descriptor, source_pins, session_id)
        startup.private(self.output)
        self.record = startup.Recorder(self.output, clone(self.inputs['environment']))
        self.record.canaries = list(harness.record.canaries)
        self.enrollment_attempted = False
        self.enrollment_uncertain = True
        self.enrolled = None
        self.capture_attempted = self.capture_complete = False
        self.validated_journal = None
        startup.document(self.output / 'inputs.json', self.inputs)

    def verify_inputs(self):
        require(same(source_inputs(self.harness, self.descriptor, self.inputs['source_pins'], self.inputs['session_id']), self.inputs)
                and same(document(self.output / 'inputs.json'), self.inputs) and self.record.env == self.inputs['environment'],
                'audit original inputs changed')

    def command(self, label, script):
        self.verify_inputs()
        index = len(self.record.receipts) + 1
        maximum = 3 + (audit.JOURNAL_LIMIT + probe.CHUNK_SIZE - 1) // probe.CHUNK_SIZE
        require(index <= maximum, 'audit command ledger exceeds bound')
        self.record.run(label, argv(self.inputs, script), cwd=Path(self.inputs['project_binding']['project_path']),
                        executable=self.inputs['cli'], timeout=TIMEOUT, observer_only=True)
        self.verify_inputs()
        return command_proof(self.output, self.inputs, index, label, script)

    def enroll(self):
        require(not self.enrollment_attempted, 'audit enrollment cannot be retried or adopted')
        self.enrollment_attempted = True
        self.enrollment_uncertain = True
        raw, receipt = self.command('enroll', probe.enrollment_script(self.inputs['session_id'], self.inputs['runtime_sha256']))
        snapshot = probe.parse_snapshot(raw, session_id=self.inputs['session_id'], runtime_sha256=self.inputs['runtime_sha256'],
                                        enrolled=True)
        proof = enrollment_proof(self.inputs, snapshot, receipt)
        startup.document(self.output / 'enrollment.json', proof)
        self.enrolled = proof
        self.enrollment_uncertain = False
        try:
            self.assert_enrolled_certain()
        except BaseException:
            self.enrollment_uncertain = True
            raise
        return proof

    def assert_enrolled_certain(self):
        require(self.enrollment_attempted and not self.enrollment_uncertain and self.enrolled is not None,
                'audit enrollment mutation unresolved; cleanup withheld')
        require(not self.capture_attempted or self.capture_complete,
                'audit final capture unresolved; cleanup withheld')
        self.verify_inputs()
        require(all(row['effects_uncertain'] is False and row['capture_complete'] is True and
                    type(row['exit_code']) is int and row['exit_code'] == 0 for row in self.record.receipts),
                'audit observer command unresolved; cleanup withheld')
        replay = ReplaySession(self)
        require(same(replay.enroll(), self.enrolled), 'audit enrollment differs from independently replayed raw bytes')

    def capture(self):
        self.assert_enrolled_certain()
        require(not self.capture_attempted, 'audit capture cannot be retried')
        self.capture_attempted = True
        proof, journal = capture_program(self, self.enrolled)
        startup.document(self.output / 'capture.json', proof)
        replay = ReplaySession(self)
        replay.enroll()
        repeated, replayed_journal = capture_program(replay, replay.enrolled)
        replay.assert_complete()
        require(same(repeated, proof) and same(replayed_journal, journal) and
                same(document(self.output / 'capture.json'), repeated), 'audit raw replay differs from retained result')
        self.verify_inputs()
        self.validated_journal = replayed_journal
        self.capture_complete = True
        return proof

    def replay(self):
        self.verify_inputs()
        replay = ReplaySession(self)
        replay.enroll()
        proof, journal = capture_program(replay, replay.enrolled)
        require(same(document(self.output / 'capture.json'), proof), 'audit retained proof differs from raw replay')
        replay.assert_complete()
        return proof, journal


def capture_program(session, enrolled):
    inputs = session.inputs
    script = probe.snapshot_script(inputs['session_id'], inputs['runtime_sha256'])
    raw, first = session.command('snapshot-before', script)
    snapshot = probe.parse_snapshot(raw, session_id=inputs['session_id'], runtime_sha256=inputs['runtime_sha256'],
                                    expected_boot_id=enrolled['boot_id'])
    probe.same_enrollment(enrolled['snapshot'], snapshot)
    count = (snapshot['journal_size'] + probe.CHUNK_SIZE - 1) // probe.CHUNK_SIZE
    require(0 < count <= (audit.JOURNAL_LIMIT + probe.CHUNK_SIZE - 1) // probe.CHUNK_SIZE,
            'audit journal empty or exceeds bounded capture')
    chunks, commands = [], [first]
    for index in range(count):
        script = probe.chunk_script(inputs['session_id'], inputs['runtime_sha256'], index,
                                    snapshot['journal_size'], snapshot['journal_sha256'])
        raw, receipt = session.command('chunk-%04d' % index, script)
        chunks.append(probe.parse_chunk(raw, snapshot=snapshot, index=index))
        commands.append(receipt)
    raw, last = session.command('snapshot-after', probe.snapshot_script(inputs['session_id'], inputs['runtime_sha256']))
    final = probe.parse_snapshot(raw, session_id=inputs['session_id'], runtime_sha256=inputs['runtime_sha256'],
                                 expected_boot_id=enrolled['boot_id'])
    probe.same_enrollment(enrolled['snapshot'], final)
    assembled = probe.assemble(snapshot, chunks, final)
    journal = assembled['validation']
    commands.append(last)
    windows = [enrolled['command'], *commands]
    require(all(before['finished_unix_ns'] <= after['started_unix_ns'] for before, after in zip(windows, windows[1:])),
            'audit source-selected command windows overlap or reorder')
    summary = {key: value for key, value in journal.items() if key != 'invocations'}
    proof = {'schema_version': 1, 'scope': SCOPE, 'session_id': inputs['session_id'], 'boot_id': enrolled['boot_id'],
             'owner': clone(inputs['descriptor']['owner']), 'incarnation_id': inputs['descriptor']['incarnation_id'],
             'incarnation_generation': inputs['descriptor']['incarnation_generation'],
             'runtime_sha256': inputs['runtime_sha256'], 'inputs_sha256': driver.sha256(evidence.canonical(inputs)),
             'enrollment_sha256': driver.sha256(evidence.canonical(enrolled)), 'snapshot': snapshot,
             'commands': commands, 'journal': summary, 'independent_raw_replay_required': True,
             'normal_Up_startup_invocations_covered': False, 'public_Stop_invocations_covered': False,
             'docker_operation_mapping_certified': False, 'full_process_absence_certified': False}
    return proof, journal


class ReplaySession:
    """Only original source-selected raw receipts: never dispatch or write."""
    def __init__(self, live):
        self.live, self.inputs, self.output = live, clone(live.inputs), live.output
        self.expected_count, self.count = len(live.record.receipts), 0
        self.labels = []
        self.enrolled = None

    def command(self, label, script):
        self.live.verify_inputs()
        require(self.count < self.expected_count, 'missing source-selected audit command')
        result = command_proof(self.output, self.inputs, self.count + 1, label, script)
        self.count += 1
        self.labels.append(label)
        return result

    def enroll(self):
        raw, receipt = self.command('enroll', probe.enrollment_script(self.inputs['session_id'], self.inputs['runtime_sha256']))
        snapshot = probe.parse_snapshot(raw, session_id=self.inputs['session_id'], runtime_sha256=self.inputs['runtime_sha256'],
                                        enrolled=True)
        self.enrolled = enrollment_proof(self.inputs, snapshot, receipt)
        require(same(document(self.output / 'enrollment.json'), self.enrolled), 'audit retained enrollment differs from raw replay')
        return self.enrolled

    def assert_complete(self):
        self.live.verify_inputs()
        require(self.count == self.expected_count and self.count >= 4, 'audit command ledger incomplete')
        names = {'inputs.json', 'enrollment.json', 'capture.json'}
        for index, label in enumerate(self.labels, 1):
            names.update('%03d-%s%s' % (index, label, suffix) for suffix in ('.intent.json', '.result.json', '.stdout', '.stderr'))
        require({path.name for path in self.output.iterdir()} == names, 'extra or missing audit session evidence')
