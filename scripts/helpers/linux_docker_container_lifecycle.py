"""Installed Machine-context container lifecycle workload; DEV, not certification.

Every resource starts with a fresh, source-derived name and is retained on an
unresolved command or assertion. No prune, daemon fallback or adoption exists.
"""
import json
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_container_fixture as fixture
import linux_docker_interactive_evidence as interactive

LABEL = 'dev.vz.container-io'
ENTRYPOINT = ['python3', '-u', '/fixture/probe.py']
ATTACH_START = ("import os,sys; assert os.read(0,1)==b'!'; "
                "os.execv('/usr/local/bin/python3',['python3','-u','/fixture/probe.py','stream',sys.argv[1]])")
# Observe only rejected predicates in the unchanged, digest-pinned probe.
# No terminal repair, initial-size wait, exception suppression or successful
# output substitution: its original predicate and exit status remain decisive.
TTY_START = '''import importlib.util,os,sys
spec=importlib.util.spec_from_file_location('vzio_probe','/fixture/probe.py')
probe=importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)
owner=probe.token(sys.argv[1])
original=probe.require
def checked(value):
    if not value:
        frame=sys._getframe(1)
        saved=frame.f_locals.get('saved')
        probe.emit('tty_contract_failure',owner,stream=sys.stderr.buffer,
            check=frame.f_code.co_name,line=frame.f_lineno,
            isatty=[os.isatty(fd) for fd in (0,1,2)],
            rows=frame.f_locals.get('rows'),cols=frame.f_locals.get('columns'),
            lflag=saved[3] if saved is not None else None)
    return original(value)
probe.require=checked
raise SystemExit(probe.main(['tty',owner]))
'''
require = driver.require


def token(inputs):
    material = json.dumps([inputs['run_id'], inputs['scope']], sort_keys=True).encode()
    return 'vzio-' + driver.sha256(material)[:24]


def io_plan(actions):
    return {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': 30,
            'input_limit': driver.MAX_STREAM_BYTES, 'output_limit': driver.MAX_STREAM_BYTES, 'actions': actions}


def build_arguments(inputs, selected, tag):
    return ['buildx', 'build', '--builder', inputs['scope']['docker_context'], '--platform', 'linux/arm64',
            '--network', 'none', '--progress', 'plain', '--load', '--no-cache', '--pull=false',
            '--build-arg', 'FIXTURE_BASE=' + inputs['images']['base']['reference'], '--tag', tag, str(selected)]


class Lifecycle(driver.Driver):
    def __init__(self, inputs, base_fixture, output, selected):
        super().__init__(inputs, base_fixture, output)
        self.selected = selected
        fixture.fixture_contract(selected)
        self.token = token(inputs.raw)
        self.tag = self.token + ':container-io'
        self.image_id = None
        self.containers = {}
        self.steps = []
        self.io_observations = None
        self.service_generation = None
        self.engine_policy = None
        self.workload_complete = False
        self.follower = None
        self.follow_proof = None
        self.register_follower = None
        self.term_generation = None
        self.term_started = None
        self.events_since = None
        self.baseline = None

    def step(self, label, args, *, expected=0, timeout=30, plan=None):
        # Append intent before the command; incomplete steps cannot disappear
        # from an in-memory cleanup decision if dispatch or persistence fails.
        row = {'label': label, 'command_index': None, 'complete': False}
        self.steps.append(row)
        result = self.command(args, expected=expected, timeout=timeout, interaction_plan=plan)
        row.update(command_index=result.index, complete=True)
        startup.document(self.output / ('step-%04d.json' % len(self.steps)), row)
        return result

    def absent(self, name, kind='container'):
        args = ['container', 'ls', '--all', '--quiet', '--no-trunc', '--filter', 'name=^/' + name + '$'] if kind == 'container' else [
            'image', 'ls', '--quiet', '--no-trunc', '--filter', 'reference=' + name]
        result = self.step('absent-' + kind + '-' + name, args)
        require(not result.stdout and not result.stderr, 'owned resource name preexists')

    def prepare(self):
        self.guard()
        initial = self.step('baseline-containers', ['container', 'ls', '--all', '--quiet', '--no-trunc'])
        self.baseline = container_ids(initial.stdout)
        require(not initial.stderr, 'baseline container inventory diagnostics')
        self.absent(self.tag, 'image')
        fixture.fixture_contract(self.selected)
        self.step('build-fixture', build_arguments(self.inputs.raw, self.selected, self.tag), timeout=300)
        result = self.step('inspect-fixture', ['image', 'inspect', self.tag])
        rows = json.loads(result.stdout)
        require(len(rows) == 1 and rows[0]['Os'] == 'linux' and rows[0]['Architecture'] == 'arm64' and
                rows[0]['Config']['Entrypoint'] == ENTRYPOINT and rows[0]['Config']['WorkingDir'] == '/workspace',
                'container fixture image differs')
        self.image_id = driver.immutable_image(rows[0]['Id'])
        policy = self.step('engine-policy', ['info', '--format', '{{json .}}'])
        self.engine_policy = json.loads(policy.stdout)
        require(self.engine_policy['ID'] == self.inputs.scope['engine_id'], 'foreign Engine policy')
        self.guard()

    def create(self, role, command, *, interactive_input=False, health=False, entrypoint=None):
        name = self.token + '-' + role
        require(role not in self.containers, 'container role already used')
        self.absent(name)
        args = ['container', 'create', '--pull', 'never', '--network', 'none', '--restart', 'no',
                '--name', name, '--label', LABEL + '=' + self.token]
        if interactive_input:
            # Explicit stdin attachment makes CLI StdinOnce=true. Merely
            # keeping detached stdin open would not prove attach half-close.
            args += ['--interactive', '--attach', 'stdin', '--attach', 'stdout', '--attach', 'stderr']
        if entrypoint is not None:
            args += ['--entrypoint', entrypoint]
        if health:
            args += ['--health-cmd', 'python3 -u /fixture/probe.py health ' + self.token,
                     '--health-interval', '1s', '--health-timeout', '1s', '--health-retries', '1',
                     '--health-start-period', '30s', '--health-start-interval', '1s']
        row = {'name': name, 'command': command, 'interactive': interactive_input, 'health': health, 'cid': None,
               'entrypoint': [entrypoint] if entrypoint is not None else ENTRYPOINT}
        self.containers[role] = row
        result = self.step('create-' + role, args + [self.image_id, *command])
        require(not result.stderr, 'container creation diagnostics')
        row['cid'] = driver.checked_text(result.stdout.decode().strip(), r'[0-9a-f]{64}', 'created container ID')
        require(result.stdout == (row['cid'] + '\n').encode(), 'ambiguous created ID')
        row['created'] = self.inspect(role, 'created')
        return row

    def inspect(self, role, status):
        from linux_docker_container_state import inspect_container
        row = self.containers[role]
        require(row['cid'] is not None, 'container creation unresolved')
        result = self.step('inspect-' + role + '-' + status, ['container', 'inspect', row['cid']])
        require(not result.stderr, 'inspect diagnostics')
        return inspect_container(result.stdout, cid=row['cid'], name=row['name'], image_id=self.image_id,
            token=self.token, command=row['command'], state=status, interactive=row['interactive'], health=row['health'],
            entrypoint=row['entrypoint'], tty=row.get('tty', False))

    def service_guard(self, cid=None, token=None):
        from linux_docker_container_state import same_identity
        self.guard()
        require(cid is None or cid == self.containers['service']['cid'] and token == self.token,
                'foreign service guard request')
        observed = self.inspect('service', 'running')
        same_identity(self.service_generation, observed)
        require(observed['State']['Pid'] == self.service_generation['State']['Pid'] and
                observed['State']['StartedAt'] == self.service_generation['State']['StartedAt'],
                'service changed generation during exec')
        return observed

    def verify_interaction(self, result, args, plan, expected):
        argv = ['docker', '--config', self.inputs.raw['docker_config'], '--context',
                self.inputs.scope['docker_context'], *args]
        return interactive.validate_recorded(self.output, result.index, argv=argv,
            executable=self.inputs.raw['clients']['docker']['path'], env=self.env,
            expected_exit=expected, expected_plan=plan)

    def attach(self):
        row = self.create('attach', ['-u', '-c', ATTACH_START, self.token], interactive_input=True, entrypoint='python3')
        require(all(row['created']['Config'].get(key) is True for key in
                    ('StdinOnce', 'AttachStdin', 'AttachStdout', 'AttachStderr')),
                'attach does not own one EOF-closing stdin attachment')
        self.step('start-attach', ['container', 'start', row['cid']])
        self.inspect('attach', 'running')
        # PID1 waits for a public kickoff byte. Therefore no probe output can
        # race ahead of attach; this is not a retrospective `logs` substitute.
        plan = io_plan([{'kind': 'write', 'data': b'!'},
            {'kind': 'write', 'data': fixture.INPUT,
             'after': {'stream': 'stderr', 'marker': fixture.marker(self.token, 'stderr-begin')}},
            {'kind': 'close_stdin'}])
        args = ['attach', row['cid']]
        result = self.step('attach-stream', args, expected=37, plan=plan)
        proof = self.verify_interaction(result, args, plan, 37)
        semantic = fixture.validate_stream(result.stdout, result.stderr, 37, self.token)
        from linux_docker_container_state import stopped
        stopped(self.inspect('attach', 'exited'), 37)
        self.record.acknowledge_negative(result, 'source-selected attach binary EOF and exact owned exit37')
        return {'capture': proof, 'semantic': semantic}

    def run_case(self, role, command, expected, *, plan=None, entrypoint=None):
        name = self.token + '-' + role
        require(role not in self.containers, 'run role repeated')
        self.absent(name)
        args = ['run', '--pull', 'never', '--network', 'none', '--restart', 'no', '--name', name,
                '--label', LABEL + '=' + self.token]
        if plan is not None:
            args += ['--interactive']
            if plan['mode'] == 'pty':
                args += ['--tty']
        if entrypoint is not None:
            args += ['--entrypoint', entrypoint]
        row = {'name': name, 'command': command, 'interactive': plan is not None, 'health': False,
               'cid': None, 'entrypoint': [entrypoint] if entrypoint is not None else ENTRYPOINT,
               'tty': plan is not None and plan['mode'] == 'pty'}
        self.containers[role] = row
        args += [self.image_id, *command]
        result = self.step('run-' + role, args, expected=expected, plan=plan)
        captured = self.verify_interaction(result, args, plan, expected) if plan is not None else None
        raw = self.step('resolve-run-' + role, ['container', 'inspect', name])
        values = json.loads(raw.stdout)
        require(len(values) == 1 and not raw.stderr, 'ambiguous run identity')
        item = values[0]
        row['cid'] = driver.checked_text(item['Id'], r'[0-9a-f]{64}', 'run container ID')
        if expected not in (126, 127):
            from linux_docker_container_state import stopped
            stopped(self.inspect(role, 'exited'), expected)
            if role == 'stdin':
                fixture.validate_stream(result.stdout, result.stderr, 37, self.token)
            elif role == 'sigint':
                fixture.validate_tty(result.stdout, 130, self.token, mode='sigint')
                require(not result.stderr, 'PTY run cannot have separate stderr')
            elif role == 'sigterm':
                fixture.validate_service(result.stdout, result.stderr, self.token, signals=('SIGTERM',), exit_code=143)
            else:
                require(not result.stdout and not result.stderr, 'numeric-exit fixture emitted bytes')
        else:
            # Failed exec never becomes a successful running/stopped generation.
            # Preserve its raw State.Error rather than normalize it away.
            require(item['Name'] == '/' + name and item['Image'] == self.image_id and
                    item['Config']['Labels'] == {LABEL: self.token} and
                    item['Config']['Entrypoint'] == [entrypoint] and not item['Config']['Cmd'] and
                    item['HostConfig']['Runtime'] == 'youki' and item['HostConfig']['NetworkMode'] == 'none' and
                    item['State']['Running'] is False and type(item['State']['Pid']) is int and
                    item['State']['Pid'] == 0 and not item['Mounts'] and
                    not result.stdout and bool(result.stderr), 'failed-command ownership/state differs')
            required = b'permission denied' if expected == 126 else b'no such file or directory'
            require(entrypoint.encode() in result.stderr and required in result.stderr.lower(),
                    'missing/nonexecutable command diagnostic differs')
        if expected:
            self.record.acknowledge_negative(result, 'source-selected run command, exact owned state and exit' + str(expected))
        return {'command_index': result.index, 'capture': captured, 'exit': expected, 'cid': row['cid']}

    def engine_clock(self, label):
        from linux_docker_container_state import timestamp
        command = self.step(label, ['info', '--format', '{{json .}}'])
        value = json.loads(command.stdout)
        require(value['ID'] == self.inputs.scope['engine_id'] and not command.stderr, 'foreign Engine clock')
        return timestamp(value['SystemTime'])

    def term_guard(self, cid, token):
        from linux_docker_container_state import same_generation
        require(cid == self.containers['term']['cid'] and token == self.token, 'foreign TERM actor')
        observed = self.inspect('term', 'running')
        same_generation(self.term_generation, observed)
        return observed

    def terminate(self):
        from linux_docker_container_state import stopped
        cid = self.containers['term']['cid']
        result = self.step('signal-term', ['kill', '--signal', 'TERM', cid])
        require(result.stdout == (cid + '\n').encode() and not result.stderr, 'TERM acknowledgement differs')
        wait = self.step('wait143', ['container', 'wait', cid])
        require(wait.stdout == b'143\n' and not wait.stderr, 'TERM guest exit differs')
        stopped(self.inspect('term', 'exited'), 143)
        self.term_started = self.record.receipts[result.index - 1]['started_unix_ns']
        return {'command_index': result.index, 'started_unix_ns': self.term_started}

    def follow_term(self):
        self.events_since = self.engine_clock('events-clock-start')
        row = self.create('term', ['service', self.token])
        self.step('start-term', ['container', 'start', row['cid']])
        self.term_generation = self.inspect('term', 'running')
        return self.follow_service(row)

    def follow_service(self, row):
        from linux_docker_container_follow import run_follow
        require(callable(self.register_follower), 'follower ownership registry required')
        def register(item):
            require(self.follower is None, 'follower already registered')
            self.follower = item
            self.register_follower(item)
        self.follow_proof = run_follow(self, row['cid'], self.token, service_guard=self.term_guard,
                                       terminate=self.terminate, register_follower=register)
        return self.follow_proof

    def health(self):
        # Polls observe transitions; they never redispatch failed operations.
        samples = []
        for desired, number in (('starting', None), ('healthy', 'USR1'), ('unhealthy', 'USR2')):
            if number:
                self.step('health-signal-' + number, ['container', 'kill', '--signal', number,
                                                    self.containers['service']['cid']])
            first_command = self.record.count + 1
            deadline = time.monotonic() + 15
            for sample in range(40):
                inspected = self.service_guard()
                # Replay runs this source program too. Its current wall clock
                # cannot establish the original polling duration: use the exact
                # retained first/last command interval, including intervening
                # sleeps, and reject late success as well as late failures.
                require(self.health_phase_elapsed(first_command) <= 15 * 10**9 and
                        time.monotonic() < deadline, 'health transition deadline')
                samples.append(inspected)
                actual = inspected['State']['Health']['Status']
                require(actual in ('starting', 'healthy', 'unhealthy'), 'unknown Engine health state')
                logs = inspected['State']['Health']['Log']
                if actual == desired and (desired != 'starting' or any(log['ExitCode'] == 1 for log in logs)):
                    break
                require(desired != 'starting' or actual == 'starting', 'health transition deadline')
                time.sleep(.25)
            else:
                raise driver.Rejected('health sample count exceeded')
        from linux_docker_container_state import health_transition
        return health_transition(samples, self.token)

    def health_phase_elapsed(self, first_command):
        """Original recorder time span; replay supplies independently read rows."""
        require(type(first_command) is int and 1 <= first_command <= self.record.count and
                len(self.record.receipts) == self.record.count, 'health command interval missing')
        rows = self.record.receipts[first_command - 1:]
        beginning, previous_end = None, None
        for index, row in enumerate(rows, first_command):
            start, elapsed = row.get('started_unix_ns'), row.get('elapsed_ns')
            require(type(row.get('index')) is int and row['index'] == index and
                    type(start) is int and start > 0 and type(elapsed) is int and elapsed >= 0 and
                    (previous_end is None or previous_end <= start), 'health command timestamps differ')
            if beginning is None:
                beginning = start
            previous_end = start + elapsed
        return previous_end - beginning

    def exercise(self):
        from linux_docker_container_state import same_identity, new_generation, stopped
        from linux_docker_container_exec import run_exec_io
        self.prepare()
        row = self.create('service', ['service', self.token], health=True)
        self.step('start-service', ['container', 'start', row['cid']])
        self.service_generation = self.inspect('service', 'running')
        policy = {key: self.engine_policy[key] for key in ('ID', 'ServerVersion', 'CgroupVersion', 'OomKillDisable')}
        same_identity(row['created'], self.service_generation, start_policy=policy,
                      engine_id=self.inputs.scope['engine_id'], start_acknowledged=True)
        health = self.health()
        self.io_observations = run_exec_io(self, row['cid'], self.token, service_guard=self.service_guard)
        self.service_guard()
        attached = self.attach()
        stdin = self.run_case('stdin', ['stream', self.token], 37,
            plan=io_plan([{'kind': 'write', 'data': fixture.INPUT}, {'kind': 'close_stdin'}]))
        exits = [self.run_case('exit' + str(code), ['exit', str(code)], code) for code in (0, 37)]
        from linux_docker_container_exec import operations
        tty_signal_plan = operations(row['cid'], self.token)[-1]['plan']
        exits.append(self.run_case('sigint', ['-u', '-c', TTY_START, self.token], 130,
                                   plan=tty_signal_plan, entrypoint='python3'))
        ready = fixture.encode({'schema_version': 1, 'type': 'service_ready', 'token': self.token,
            'pid': 1, 'health': 'starting', 'output': 'stdout'}) + b'\n'
        exits.append(self.run_case('sigterm', ['service', self.token], 143,
            plan=io_plan([{'kind': 'close_stdin'}, {'kind': 'signal', 'name': 'SIGTERM',
                         'after': {'stream': 'stdout', 'marker': ready}}])))
        exits.append(self.run_case('sigkill', ['-c', 'import os,signal;os.kill(os.getpid(),signal.SIGKILL)'],
                                  137, entrypoint='python3'))
        exits += [self.run_case('nonexec', [], 126, entrypoint='/fixture/not-executable'),
                  self.run_case('missing', [], 127, entrypoint='/fixture/does-not-exist')]
        self.step('stop-service', ['container', 'stop', '--timeout', '10', row['cid']])
        stopped(self.inspect('service', 'exited'), 143)
        self.step('restart-service', ['container', 'restart', '--timeout', '10', row['cid']])
        restarted = self.inspect('service', 'running')
        new_generation(self.service_generation, restarted)
        self.service_generation = restarted
        # A host 'container kill' is the signal cause; a fixture exit(137) is not.
        self.step('kill-service', ['container', 'kill', '--signal', 'KILL', row['cid']])
        stopped(self.inspect('service', 'exited'), 137)
        wait = self.create('wait', ['exit', '37'])
        self.step('start-wait', ['container', 'start', wait['cid']])
        waited = self.step('wait37', ['container', 'wait', wait['cid']])
        require(waited.stdout == b'37\n' and not waited.stderr, 'wait did not return guest37')
        stopped(self.inspect('wait', 'exited'), 37)
        followed = self.follow_term()
        self.workload_complete = True
        return {'scope': 'DEV_CONTAINER_LIFECYCLE_WORKLOAD_NOT_RELEASE_CERTIFICATION',
                'health': health, 'exec_io': self.io_observations, 'attach': attached, 'stdin': stdin, 'exits': exits,
                'follow': followed,
                'remaining_acceptance': ['full-process-runtime-inventory', 'aggregate-release-integration']}

    def cleanup(self):
        from linux_docker_container_state import EVENT_FORMAT, validate_events
        require(self.workload_complete and all(x['complete'] for x in self.steps) and
                all(not x['effects_uncertain'] for x in self.record.receipts), 'unresolved lifecycle workload')
        require(self.follower is not None and not self.follower.follow_thread.is_alive() and
                all(not x['effects_uncertain'] for x in self.follower.record.receipts), 'unresolved follower')
        events = None
        for role, row in reversed(list(self.containers.items())):
            raw = self.step('cleanup-inspect-' + role, ['container', 'inspect', row['cid']])
            item = json.loads(raw.stdout)
            require(len(item) == 1 and not raw.stderr and item[0]['Id'] == row['cid'] and
                    item[0]['Name'] == '/' + row['name'] and item[0]['Image'] == self.image_id and
                    item[0]['Config']['Labels'] == {LABEL: self.token} and
                    item[0]['State']['Running'] is False and type(item[0]['State']['Pid']) is int and
                    item[0]['State']['Pid'] == 0, 'cleanup target ownership/nonrunning state differs')
            removed = self.step('remove-' + role, ['container', 'rm', row['cid']])
            require(removed.stdout == (row['cid'] + '\n').encode() and not removed.stderr, 'remove acknowledgement differs')
            self.absent(row['name'])
            if role == 'term':
                until = self.engine_clock('events-clock-end')
                result = self.step('events', ['events', '--since', engine_time(self.events_since), '--until', engine_time(until),
                    '--filter', 'type=container', '--filter', 'container=' + row['cid'],
                    '--filter', 'label=' + LABEL + '=' + self.token, '--format', EVENT_FORMAT])
                require(not result.stderr, 'events diagnostics')
                events = validate_events(result.stdout, cid=row['cid'], name=row['name'], image=self.image_id,
                                          token=self.token, since=self.events_since, until=until)
        inventory = self.step('final-containers', ['container', 'ls', '--all', '--quiet', '--no-trunc'])
        require(container_ids(inventory.stdout) == self.baseline and not inventory.stderr,
                'owned container remains or unrelated identity changed')
        inspected = self.step('cleanup-image-inspect', ['image', 'inspect', self.tag])
        rows = json.loads(inspected.stdout)
        require(len(rows) == 1 and rows[0]['Id'] == self.image_id, 'fixture image changed before cleanup')
        self.step('remove-fixture-image', ['image', 'rm', self.tag])
        self.absent(self.tag, 'image')
        self.guard()
        return {'events': events, 'containers_absent_and_unrelated_ids_unchanged': True,
                'fixture_tag_absent': True, 'full_process_absence_certified': False}


def container_ids(raw):
    values = raw.decode().splitlines()
    require(len(values) <= 256 and len(values) == len(set(values)) and
            all(driver.checked_text(v, r'[0-9a-f]{64}', 'container inventory ID') for v in values),
            'container inventory differs')
    return sorted(values)


def engine_time(value):
    require(type(value) is int and value > 0, 'invalid Engine time')
    return '%d.%09d' % divmod(value, 1000000000)


class ReplayRecord:
    """No-dispatch acknowledgement reader used by source-program replay."""
    def __init__(self, output):
        self.root = output
        self.receipts = []
        self.count = 0
        self.max_stream_bytes = driver.MAX_STREAM_BYTES
        self.pending_interactions = []

    def acknowledge_negative(self, command, assertion):
        path = self.root / ('command-%05d' % command.index)
        ack = interactive.parse(driver.regular(Path(str(path) + '.acknowledgement.json')))
        require(interactive.canonical(ack) == interactive.canonical({'command_index': command.index,
                'assertion': assertion, 'terminal_receipt_sha256': driver.sha256(driver.regular(Path(str(path) + '.json'))),
                'effects_uncertain': False}), 'source-derived semantic acknowledgement differs')
        require(command.returncode > 0, 'unexpected negative acknowledgement')
        self.receipts[command.index - 1]['effects_uncertain'] = False


class ReplayLifecycle(Lifecycle):
    """Re-execute the fixed workload program against independently read bytes.

    The dispatcher and all filesystem-writing entry points are overridden. The
    source chooses argv/actions/order again; recorded steps cannot choose them.
    Pure state/command/capture validators were implemented separately.
    """
    def __init__(self, live):
        self.inputs, self.output, self.fixture, self.selected = live.inputs, live.output, live.fixture, live.selected
        self.env = dict(live.env)
        self.token, self.tag = token(self.inputs.raw), token(self.inputs.raw) + ':container-io'
        self.image_id = None
        self.containers, self.steps = {}, []
        self.io_observations = self.service_generation = self.engine_policy = None
        self.workload_complete = False
        self.follower = self.follow_proof = self.term_generation = self.term_started = None
        self.events_since = self.baseline = None
        self.record = ReplayRecord(self.output)
        self.expected_count = live.record.count
        require(json.loads(driver.regular(self.output / 'inputs.json')) == self.inputs.raw, 'foreign lifecycle inputs')

    def command(self, args, *, expected=0, timeout=120, env=None, interaction_plan=None):
        from linux_docker_container_commands import validate_command
        index = self.record.count + 1
        require(index <= self.expected_count, 'missing source-selected lifecycle command')
        path = self.output / ('command-%05d.json' % index)
        row = interactive.parse(driver.regular(path, 8 * 1024 * 1024))
        code = row['exit_code'] if expected is None else expected
        if interaction_plan is not None:
            interactive.validate_recorded(self.output, index,
                argv=['docker', '--config', self.inputs.raw['docker_config'], '--context',
                      self.inputs.scope['docker_context'], *args], executable=self.inputs.raw['clients']['docker']['path'],
                env=self.env, extra_env=env, expected_exit=code, expected_plan=interaction_plan)
            stdout, stderr = [driver.regular(self.output / ('command-%05d.%s' % (index, name)), driver.MAX_STREAM_BYTES)
                              for name in ('stdout', 'stderr')]
        else:
            proof = validate_command(self.output, index, self.inputs.raw, args=args, expected_exit=code,
                extra_env=env, require_ack=False, expected_timeout_seconds=timeout)
            row, stdout, stderr = proof['receipt'], proof['stdout'], proof['stderr']
        self.record.count = index
        self.record.receipts.append(dict(row))
        return driver.Command(index, row['argv'], code, stdout, stderr)

    def guard(self):
        from linux_docker_container_commands import validate_guard
        index = self.record.count + 1
        require(index + 1 <= self.expected_count, 'missing lifecycle guard')
        proof = validate_guard(self.output, self.inputs.raw, index, index + 1)
        self.record.receipts.extend(dict(row['receipt']) for row in proof['commands'])
        self.record.count += 2

    def step(self, label, args, *, expected=0, timeout=30, plan=None):
        result = self.command(args, expected=expected, timeout=timeout, interaction_plan=plan)
        row = {'label': label, 'command_index': result.index, 'complete': True}
        self.steps.append(row)
        stored = interactive.parse(driver.regular(self.output / ('step-%04d.json' % len(self.steps))))
        require(interactive.canonical(stored) == interactive.canonical(row), 'source-selected lifecycle step differs')
        return result

    def follow_service(self, row):
        from linux_docker_container_commands import validate_guard
        from linux_docker_container_follow import replay_follow
        # Root-side ordering from run_follow; child observer has a separate
        # ledger so concurrent commands never share a mutable command index.
        self.guard(); self.term_guard(row['cid'], self.token)
        output = self.output / 'follow'
        first = validate_guard(output, self.inputs.raw, 1, 2)
        self.guard(); self.term_guard(row['cid'], self.token)
        self.terminate()
        last = validate_guard(output, self.inputs.raw, 4, 5)
        self.guard()
        expected = interactive.parse(driver.regular(self.output / 'workload.json', 8 * 1024 * 1024))['follow']
        environment = self.env | {'TMPDIR': str(output / 'private-tmp')}
        self.follow_proof = replay_follow(output, self.inputs.raw, row['cid'], self.token, expected,
            environment=environment, termination_started_unix_ns=self.term_started)
        command = interactive.parse(driver.regular(output / 'command-00003.json', 8 * 1024 * 1024))
        disposition = interactive.parse(driver.regular(output / 'follow-disposition.json'))
        require(disposition == {'schema_version': 1, 'thread_joined': True, 'capture_error_type': None,
                                'orchestration_error_type': None, 'pending_interactions': 0},
                'follower thread/process disposition differs')
        require(command_indices(output) == list(range(1, 6)), 'unexpected follower commands')
        self.follower = SimpleNamespace(follow_thread=SimpleNamespace(is_alive=lambda: False),
            record=SimpleNamespace(receipts=[*(r['receipt'] for r in first['commands']), command,
                                            *(r['receipt'] for r in last['commands'])]))
        return self.follow_proof


def command_indices(output):
    result = []
    for path in output.iterdir():
        match = re.fullmatch(r'command-([0-9]{5})\.json', path.name)
        if match:
            require(len(result) < 4096, 'command ledger exceeds bound')
            result.append(int(match[1]))
    return sorted(result)


def replay(live, *, cleanup):
    selected = ReplayLifecycle(live)
    workload = selected.exercise()
    require(workload == interactive.parse(driver.regular(live.output / 'workload.json', 8 * 1024 * 1024)),
            'replayed workload differs from retained result')
    removed = selected.cleanup() if cleanup else None
    require(selected.record.count == live.record.count and selected.steps == live.steps and
            command_indices(live.output) == list(range(1, live.record.count + 1)) and
            all(not row['effects_uncertain'] for row in selected.record.receipts),
            'extra, missing or unresolved lifecycle command')
    return {'workload': workload, 'cleanup': removed, 'command_count': selected.record.count,
            'scope': 'DEV_source_selected_lifecycle_replay_not_release_certification'}


def run_machine(harness, descriptor, scope, proof, images, index):
    from linux_docker_e2e import input_mapping
    inputs = input_mapping(harness, scope, proof, images)
    admitted = driver.Inputs(inputs, suite='compose')
    admitted.verify_runtime_evidence()
    selected = Lifecycle(admitted, Path(harness.info['fixture']),
                         harness.evidence / ('container-machine-' + str(index)),
                         Path(harness.info['container_fixture']))
    harness.drivers.append(selected)
    harness.driver_cleanup_verified.append(False)
    position = len(harness.drivers) - 1
    follower_positions = []
    def register(item):
        follower_positions.append(len(harness.drivers))
        harness.drivers.append(item)
        harness.driver_cleanup_verified.append(False)
    selected.register_follower = register
    startup.document(selected.output / 'inputs.json', inputs)
    result = selected.exercise()
    startup.document(selected.output / 'workload.json', result)
    before = replay(selected, cleanup=False)
    cleanup = selected.cleanup()
    startup.document(selected.output / 'cleanup.json', cleanup)
    after = replay(selected, cleanup=True)
    require(before['workload'] == result and after['workload'] == result and after['cleanup'] == cleanup,
            'independent lifecycle replay differs')
    harness.driver_cleanup_verified[position] = True
    require(len(follower_positions) == 1, 'exactly one owned follower required')
    harness.driver_cleanup_verified[follower_positions[0]] = True
    return {'workload': result, 'cleanup': cleanup, 'independent_validation': after}
