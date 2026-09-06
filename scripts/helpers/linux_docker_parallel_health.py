"""Owned unrelated HTTP service; independent one-second DEV health evidence."""
import hashlib
import json
from pathlib import Path
import threading

import docker_host_driver as driver
import installed_developer_startup as startup

require = driver.require
FIXTURE = Path(__file__).resolve().parents[2] / 'tests/fixtures/vz-0.4/docker-parallel'
TIMING = {'samples': 60, 'interval_ns': 1_000_000_000, 'max_lateness_ns': 250_000_000,
          'request_timeout_ns': 500_000_000, 'observer_bound_ns': 70_000_000_000}
LIMIT = 128 * 1024
LABEL = 'dev.vz.linux-compose-proof'


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate health JSON field')
        row[key] = value
    return row


def validate(raw, stderr, token, timing, run_intervals):
    """Bracket authenticated conservative whole-RUN envelopes in guest time.

    The positional intervals are [C-D, S+D], not displayed Buildx timestamps:
    S/C are the authenticated guest script start/completion and D is the exact
    RUN duration preserved by pinned Buildx's constant per-solve translation.
    Envelope construction, Engine bounds and overlap proof belong to the slot
    and group validators; this validator must cover each supplied whole envelope.
    """
    require(timing == TIMING and all(type(v) is int for v in timing.values()), 'unknown health timing contract')
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT and raw.endswith(b'\n') and stderr == b'',
            'incomplete, oversized or erroneous health stream')
    lines = raw.splitlines()
    require(len(lines) == timing['samples'] + 2, 'health sample count differs')
    rows = [json.loads(line, object_pairs_hook=unique) for line in lines]
    first, last = rows[0], rows[-1]
    require(type(first) is dict and set(first) == {'type', 'schema_version', 'token', 'pid', 'unix_ns', 'monotonic_ns', 'timing'}
            and first['type'] == 'start' and type(first['schema_version']) is int and first['schema_version'] == 1
            and first['token'] == token and first['timing'] == timing, 'foreign health start frame')
    require(all(type(first[k]) is int and first[k] > 0 for k in ('pid', 'unix_ns', 'monotonic_ns')),
            'invalid health observer identity/clock')
    require(type(last) is dict and set(last) == {'type', 'samples', 'monotonic_ns', 'unix_ns'}
            and last['type'] == 'end' and type(last['samples']) is int and last['samples'] == timing['samples'],
            'incomplete health terminal frame')
    keys = {'type', 'sequence', 'planned_monotonic_ns', 'started_monotonic_ns', 'finished_monotonic_ns',
            'started_unix_ns', 'finished_unix_ns', 'status', 'body'}
    previous_mono, previous_wall = first['monotonic_ns'], first['unix_ns']
    for index, sample in enumerate(rows[1:-1]):
        require(type(sample) is dict and set(sample) == keys and sample['type'] == 'sample', 'unknown health sample schema')
        require(all(type(sample[k]) is int for k in keys - {'type', 'body'}), 'invalid health sample numeric type')
        planned = first['monotonic_ns'] + index * timing['interval_ns']
        begin, end = sample['started_monotonic_ns'], sample['finished_monotonic_ns']
        wall, finished = sample['started_unix_ns'], sample['finished_unix_ns']
        require(sample['sequence'] == index and sample['planned_monotonic_ns'] == planned and
                previous_mono <= begin <= end and previous_wall <= wall <= finished and
                0 <= begin - planned <= timing['max_lateness_ns'] and
                end - begin <= timing['request_timeout_ns'], 'missed, reordered or slow health sample')
        require(abs((wall - first['unix_ns']) - (begin - first['monotonic_ns'])) <= timing['max_lateness_ns'] and
                abs((finished - first['unix_ns']) - (end - first['monotonic_ns'])) <= timing['max_lateness_ns'],
                'health wall/monotonic clock drift')
        require(sample['status'] == 200 and sample['body'] == token + '\n', 'wrong HTTP status or host-written marker')
        previous_mono, previous_wall = end, finished
    require(all(type(last[k]) is int for k in ('monotonic_ns', 'unix_ns')) and
            previous_mono <= last['monotonic_ns'] <= first['monotonic_ns'] + timing['observer_bound_ns'] and
            previous_wall <= last['unix_ns'] and
            abs((last['unix_ns'] - first['unix_ns']) - (last['monotonic_ns'] - first['monotonic_ns'])) <= timing['max_lateness_ns'],
            'invalid health terminal clock/deadline')
    require(type(run_intervals) in (list, tuple) and len(run_intervals) == 4,
            'four authenticated guest RUN envelopes required')
    for interval in run_intervals:
        require(type(interval) in (list, tuple) and len(interval) == 2 and
                all(type(value) is int for value in interval) and
                rows[1]['finished_unix_ns'] <= interval[0] < interval[1] <= rows[-2]['started_unix_ns'],
                'health samples do not bracket each conservative guest RUN envelope')
    return {'schema_version': 1, 'scope': 'OWNED_LOOPBACK_HTTP_DURING_FOUR_RUNS_NOT_NETWORK_CONFORMANCE',
            'samples': timing['samples'], 'sample_errors': 0, 'missed_deadlines': 0,
            'first_sample_unix_ns': rows[1]['started_unix_ns'], 'last_sample_unix_ns': rows[-2]['finished_unix_ns'],
            'guest_run_envelopes': [list(x) for x in run_intervals],
            'clock_basis': 'SAME_MACHINE_GUEST_WALL_TIME_NOT_SHIFTED_BUILDX_DISPLAY_TIME',
            'run_coverage': 'CONSERVATIVE_WHOLE_RUN_ENVELOPES_FROM_GUEST_SCRIPT_AND_PRESERVED_DURATION',
            'timing': dict(timing),
            'stdout_sha256': hashlib.sha256(raw).hexdigest(), 'stderr_sha256': hashlib.sha256(stderr).hexdigest()}


def validate_record(output, expected, token, timing, run_intervals):
    """Independently bind persisted process intent/result and full raw streams."""
    def read(name):
        return startup.read_private_regular(output / name, LIMIT)
    def document(name):
        return json.loads(read(name), object_pairs_hook=unique)
    require(document('observer-input.json') == expected, 'health observer input binding changed')
    intent = document('001-http-health.intent.json')
    result = document('001-http-health.result.json')
    fixed = {'index': 1, 'label': 'http-health', 'argv': expected['argv'], 'argv0': 'docker',
             'executable': expected['executable'], 'cwd': expected['cwd'], 'timeout_seconds': 75,
             'termination_scope': 'owned_host_process_group'}
    require(set(intent) == set(fixed) | {'started_unix_ns', 'effects_uncertain', 'capture_complete'} and
            all(intent[k] == v for k, v in fixed.items()) and
            type(intent['index']) is int and type(intent['timeout_seconds']) is int and
            type(intent['started_unix_ns']) is int and intent['started_unix_ns'] > 0 and
            intent['effects_uncertain'] is True and intent['capture_complete'] is False,
            'foreign health command intent')
    extra = {'exit_code', 'elapsed_ns', 'error', 'stdout_sha256', 'stderr_sha256',
             'retained_stdout_bytes', 'retained_stderr_bytes', 'hashes_cover'}
    require(set(result) == set(intent) | extra and
            all(result[k] == intent[k] for k in set(intent) - {'effects_uncertain', 'capture_complete'}) and
            result['effects_uncertain'] is False and result['capture_complete'] is True and
            type(result['exit_code']) is int and result['exit_code'] == 0 and result['error'] is None and
            type(result['elapsed_ns']) is int and 0 < result['elapsed_ns'] <= 75 * 10**9 and
            result['hashes_cover'] == 'complete_streams', 'health command did not complete exactly')
    raw, error = read('001-http-health.stdout'), read('001-http-health.stderr')
    for name, content in (('stdout', raw), ('stderr', error)):
        require(type(result['retained_' + name + '_bytes']) is int and
                result['retained_' + name + '_bytes'] == len(content) and
                result[name + '_sha256'] == hashlib.sha256(content).hexdigest(), 'health raw capture digest/size differs')
    proof = validate(raw, error, token, timing, run_intervals)
    proof.update(command_intent_sha256=hashlib.sha256(read('001-http-health.intent.json')).hexdigest(),
                 command_result_sha256=hashlib.sha256(read('001-http-health.result.json')).hexdigest(),
                 observer_input_sha256=hashlib.sha256(read('observer-input.json')).hexdigest())
    return proof


class Health:
    def __init__(self, harness, descriptor, images, index):
        self.harness, self.descriptor = harness, json.loads(json.dumps(descriptor))
        self.images = json.loads(json.dumps(images))
        require(type(index) is int and 0 <= index < 3, 'invalid health Machine index')
        self.output = startup.private(harness.evidence / ('parallel-health-' + str(index)))
        self.environment = dict(harness.env)
        self.record = startup.Recorder(self.output, self.environment)
        self.thread, self.error, self.result = None, None, None
        self.prepared, self.started = False, False
        self.fixture = Path(harness.info['parallel_fixture'])
        self.source = driver.regular(self.fixture / 'health.py', LIMIT).decode()
        self.contract_raw = driver.regular(self.fixture / 'contract.json', LIMIT)
        contract = json.loads(self.contract_raw, object_pairs_hook=unique)
        require(contract['health'] == TIMING, 'health fixture contract differs')

    def verify_inputs(self):
        require(driver.regular(self.fixture / 'health.py', LIMIT) == self.source.encode() and
                driver.regular(self.fixture / 'contract.json', LIMIT) == self.contract_raw and
                self.harness.env == self.environment, 'selected health source/contract/environment changed')

    def route(self):
        raw, error, _ = self.harness.docker('health-context', self.descriptor,
                                          ['context', 'inspect', self.descriptor['name']])
        contexts = json.loads(raw)
        require(not error and len(contexts) == 1 and contexts[0]['Name'] == self.descriptor['name'] and
                contexts[0]['Endpoints']['docker']['Host'] == self.descriptor['endpoint'], 'health context rerouted')
        raw, error, _ = self.harness.docker('health-engine', self.descriptor, ['info', '--format', '{{.ID}}'])
        require(not error and raw.decode().strip() == self.descriptor['engine_id'], 'health Engine changed')

    def inspect(self, label):
        raw, error, _ = self.harness.docker(label, self.descriptor, ['container', 'inspect', self.container_id])
        items = json.loads(raw)
        require(not error and len(items) == 1, 'ambiguous health service inspection')
        item = items[0]
        command = ['-u', '-c', self.source, 'serve']
        require(item['Id'] == self.container_id and item['Image'] == self.images['compose']['id'] and
                item['Name'] == '/' + self.token and item['Config']['Labels'][LABEL] == self.token and
                item['Path'] == 'python3' and item['Args'] == command and
                item['Config']['Entrypoint'] == ['python3'] and item['Config']['Cmd'] == command and
                item['HostConfig']['NetworkMode'] == 'none' and item['HostConfig']['Runtime'] == 'youki' and
                item['RestartCount'] == 0 and
                item['State']['Running'] is True and item['State']['Status'] == 'running' and
                type(item['State']['Pid']) is int and item['State']['Pid'] > 0 and
                all(item['State'][k] is False for k in ('Paused', 'Restarting', 'Dead', 'OOMKilled')),
                'health service identity or lifecycle changed')
        return item

    def prepare(self):
        require(not self.prepared and self.thread is None, 'health preparation cannot repeat')
        h = self.harness
        matches = [row for row in h.owned if row.get('descriptor') == self.descriptor and
                   row.get('image_id') == self.images['compose']['id'] and row.get('kind') != 'sentinel']
        require(len(matches) == 1 and not matches[0].get('container_id'), 'exact unused owned health image required')
        row = matches[0]
        self.verify_inputs()
        self.route()
        self.token = row['token']
        h.exact_absent(self.descriptor, 'container', self.token)
        raw, error, _ = h.mutate('health-container-create', self.descriptor,
            ['container', 'create', '--network', 'none', '--label', LABEL + '=' + self.token,
             '--name', self.token, '--entrypoint', 'python3', self.images['compose']['id'], '-u', '-c', self.source, 'serve'])
        self.container_id = driver.checked_text(raw.decode().strip(), r'[0-9a-f]{64}', 'health container ID')
        row['container_id'] = self.container_id
        require(not error, 'health create error stream')
        h.mutate('health-container-start', self.descriptor, ['container', 'start', self.container_id])
        marker = 'from pathlib import Path; p=Path("/tmp/vz-parallel-health-marker"); f=p.open("x"); f.write(' + repr(self.token + '\n') + '); f.close()'
        h.mutate('health-host-marker', self.descriptor, ['exec', self.container_id, 'python3', '-c', marker])
        # One explicit bounded provisioning wait, not a measured HTTP retry.
        ready = ('import time\nfrom pathlib import Path\np=Path("/tmp/vz-parallel-health-ready")\n'
                 'end=time.monotonic()+5\nwhile not p.exists() and time.monotonic()<end: time.sleep(.01)\n'
                 'assert p.read_bytes()==b"ready\\n"\n')
        raw, error, _ = h.docker('health-service-ready', self.descriptor,
                               ['exec', self.container_id, 'python3', '-c', ready], timeout=10)
        require(not raw and not error, 'health service did not become ready')
        self.before = self.inspect('health-service-before')
        startup.document(self.output / 'ownership.json', {'descriptor': self.descriptor, 'token': self.token,
            'container_id': self.container_id, 'image_id': self.images['compose']['id'],
            'script_sha256': hashlib.sha256(self.source.encode()).hexdigest(), 'timing': TIMING, 'before': self.before})
        self.prepared = True

    def start(self):
        require(self.prepared and self.thread is None, 'health observer must be prepared and single-use')
        self.verify_inputs()
        argv = ['docker', '--config', str(self.harness.config), '--context', self.descriptor['name'],
                'exec', self.container_id, 'python3', '-u', '-c', self.source, 'probe', self.token,
                json.dumps(TIMING, sort_keys=True, separators=(',', ':'))]
        self.observer_input = {'schema_version': 1, 'descriptor': self.descriptor, 'argv': argv,
            'executable': self.harness.info['clients']['docker']['canonical'], 'cwd': str(self.harness.root),
            'environment': self.environment, 'script_sha256': hashlib.sha256(self.source.encode()).hexdigest(),
            'contract_sha256': hashlib.sha256(self.contract_raw).hexdigest(), 'timing': dict(TIMING)}
        startup.document(self.output / 'observer-input.json', self.observer_input)
        def observe():
            try:
                self.result = self.record.run('http-health', argv,
                    executable=self.observer_input['executable'], cwd=self.harness.root, timeout=75)
            except BaseException as error:
                self.error = error
        self.thread = threading.Thread(target=observe, name='vz-parallel-http-health', daemon=False)
        self.thread.start()
        self.started = True

    def finish(self, run_intervals):
        # Always positively join first, even when callers pass [] after failure.
        if self.thread is not None and self.thread.ident is not None:
            self.thread.join(timeout=85)
            require(not self.thread.is_alive(), 'health observer did not positively terminate; cleanup withheld')
        require(self.started and self.result is not None and self.error is None,
                'health observer failed or never completed: ' + repr(self.error))
        raw, error, code = self.result
        require(code == 0 and len(self.record.receipts) == 1 and
                self.record.receipts[0]['capture_complete'] is True and
                self.record.receipts[0]['effects_uncertain'] is False, 'health observer completion unproven')
        self.verify_inputs()
        proof = validate_record(self.output, self.observer_input, self.token, TIMING, run_intervals)
        self.route()
        after = self.inspect('health-service-after')
        for key in ('Id', 'Name', 'Image', 'Config', 'HostConfig'):
            require(after[key] == self.before[key], 'health service configuration changed')
        require(after['State']['Pid'] == self.before['State']['Pid'] and
                after['State']['StartedAt'] == self.before['State']['StartedAt'], 'health service restarted or replaced')
        proof.update(owner=self.descriptor['owner'], context=self.descriptor['name'],
                     container_id=self.container_id, before=self.before, after=after)
        startup.document(self.output / 'health-validation.json', proof)
        return proof
