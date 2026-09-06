"""Source-selected tiny image lifecycle and independent no-dispatch replay.

Pass a fresh, already authenticated/registered Driver. exercise() retains its
owned decoy; cleanup() is a separate explicitly called final stage. Exceptions
never invoke cleanup, retries, force removal, prune, registry or fallback paths.
This is a DEV recipe, not installed-Machine or full Docker certification.
"""
from pathlib import Path
from types import SimpleNamespace
import re

import docker_host_driver as driver
import linux_docker_container_commands as commands
import linux_docker_interactive_evidence as interactive
import linux_docker_image_archive as archive_verifier
import linux_docker_image_fixture as fixture

MAX_IMAGES = 256
MAX_COMMANDS = 4096
TIMEOUT = 30
require = driver.require


def canonical(value):
    return interactive.canonical(value)


def references(inputs):
    require(type(inputs.get('run_id')) is str and re.fullmatch('[A-Za-z0-9_-]{1,128}', inputs['run_id']),
            'source-selected image run identity required')
    scope = inputs['scope']
    require(type(scope) is dict and all(type(scope.get(key)) is str and scope[key] for key in
            ('docker_context', 'docker_endpoint', 'engine_id', 'machine_id')), 'exact Machine image scope required')
    suffix = fixture.sha256(canonical({'run_id': inputs['run_id'], 'scope': scope}))[:24]
    repository = 'docker.io/library/vz-image-' + suffix
    return {role: repository + ':' + role for role in ('source', 'alias', 'decoy')}


def io_plan(payload=None):
    actions = ([] if payload is None else [{'kind': 'write', 'data': payload}]) + [{'kind': 'close_stdin'}]
    return {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': TIMEOUT,
            'input_limit': max(1, len(payload) if payload is not None else 0),
            'output_limit': driver.MAX_STREAM_BYTES, 'actions': actions}


def validate_archive(raw, role, reference):
    expected = fixture.fixture(role)
    return archive_verifier.validate(raw, expected_manifest_digest=expected['manifest_digest'],
        expected_config_digest=expected['config_digest'], expected_layer_digest=expected['layer_digest'],
        expected_diff_id=expected['diff_id'], expected_reference=reference,
        expected_payload_path=expected['payload']['path'], expected_payload_sha256=expected['payload']['sha256'],
        expected_payload_size=expected['payload']['size'], expected_labels=expected['labels'])


def image_ids(raw):
    require(type(raw) is bytes and len(raw) <= 65536 and (not raw or raw.endswith(b'\n')), 'image inventory bounds')
    lines = raw.splitlines()
    require(len(lines) <= MAX_IMAGES * 4 and all(re.fullmatch(b'sha256:[0-9a-f]{64}', line) for line in lines),
            'image inventory format/count')
    ids = sorted({line.decode('ascii') for line in lines})
    require(len(ids) <= MAX_IMAGES + 2, 'image identity inventory bound')
    return ids


def projection(row):
    require(type(row) is dict and type(row.get('Id')) is str and re.fullmatch('sha256:[0-9a-f]{64}', row['Id']),
            'image inspect identity')
    result = {key: row.get(key) for key in ('Id', 'Config', 'RootFS', 'Architecture', 'Os', 'Variant', 'Created')}
    require(type(result['Config']) is dict and type(result['RootFS']) is dict and
            all(type(result[key]) is str for key in ('Architecture', 'Os', 'Created')), 'image inspect config/platform')
    for key in ('RepoTags', 'RepoDigests'):
        values = row.get(key)
        if values is None:
            values = []
        require(type(values) is list and len(values) <= 256 and
                all(type(value) is str and 0 < len(value) <= 512 and '\0' not in value for value in values) and
                len(set(values)) == len(values), 'image reference inventory')
        result[key] = sorted(values)
    return result


def owned_image(row, role, refs):
    expected = fixture.fixture(role)
    require(row['Id'] == expected['manifest_digest'] and row['Architecture'] == 'arm64' and row['Os'] == 'linux' and
            row['Variant'] in (None, '', 'v8') and row['Created'] == fixture.CREATED and
            row['RootFS'] == {'Type': 'layers', 'Layers': [expected['diff_id']]}, 'owned image content/platform differs')
    familiar = sorted(fixture.familiar_reference(ref) for ref in refs)
    repository = familiar[0].rsplit(':', 1)[0]
    require(all(value.rsplit(':', 1)[0] == repository for value in familiar) and row['RepoTags'] == familiar and
            row['RepoDigests'] == [repository + '@' + expected['manifest_digest']], 'owned image references differ')
    config = row['Config']
    require(config.get('Labels') == expected['labels'] and config.get('WorkingDir') == '/', 'owned image labels/config differ')
    # An OCI seed has no executable/environment/user/healthcheck settings. Go's
    # Docker config projection may retain explicit empty scalar/collection fields.
    empty = {'Hostname': '', 'Domainname': '', 'User': '', 'AttachStdin': False, 'AttachStdout': False,
             'AttachStderr': False, 'Tty': False, 'OpenStdin': False, 'StdinOnce': False,
             'ArgsEscaped': False, 'Image': '', 'NetworkDisabled': False, 'MacAddress': '', 'StopSignal': '',
             'ExposedPorts': None, 'Env': None, 'Cmd': None, 'Healthcheck': None, 'Volumes': None,
             'Entrypoint': None, 'OnBuild': None, 'StopTimeout': None, 'Shell': None}
    for key, value in config.items():
        if key not in ('Labels', 'WorkingDir'):
            require(key in empty and canonical(value) == canonical(empty[key]), 'unexpected nonempty image config')
    return row


class _Live:
    def __init__(self, item):
        self.item, self.inputs, self.output = item, item.inputs.raw, Path(item.output)
        self.env = dict(item.env)
        require(item.record.count == 0 and item.record.max_stream_bytes == driver.MAX_STREAM_BYTES,
                'fresh bounded image Driver required')
        self.count = 0

    def command(self, args, *, plan=None):
        require(self.count + 1 <= MAX_COMMANDS, 'image command bound before dispatch')
        result = self.item.command(args, expected=0, timeout=TIMEOUT, interaction_plan=plan)
        require(result.index == self.count + 1 and result.index <= MAX_COMMANDS, 'image command order/bound')
        proof = _read_command(self.output, self.inputs, self.env, result.index, args, plan)
        require(result.stdout == proof.stdout and result.stderr == proof.stderr, 'image command readback differs')
        self.count = result.index
        return proof

    def guard(self):
        index = self.count + 1
        require(index + 1 <= MAX_COMMANDS, 'image guard bound before dispatch')
        self.item.guard()
        commands.validate_guard(self.output, self.inputs, index, index + 1)
        require(self.item.record.count == index + 1 and index + 1 <= MAX_COMMANDS, 'image guard count')
        self.count += 2


def _read_command(output, inputs, environment, index, args, plan):
    if plan is None:
        proof = commands.validate_command(output, index, inputs, args=args, expected_exit=0,
                                          expected_timeout_seconds=TIMEOUT)
        stdout, stderr = proof['stdout'], proof['stderr']
        receipt = proof['receipt']
    else:
        proof = interactive.validate_recorded(output, index,
            argv=['docker', '--config', inputs['docker_config'], '--context', inputs['scope']['docker_context'], *args],
            executable=inputs['clients']['docker']['path'], env=environment,
            expected_exit=0, expected_plan=plan)
        stdout, stderr = [driver.regular(output / ('command-%05d.%s' % (index, name)), driver.MAX_STREAM_BYTES)
                          for name in ('stdout', 'stderr')]
        raw_receipt = driver.regular(output / ('command-%05d.json' % index), driver.MAX_STREAM_BYTES)
        require(driver.sha256(raw_receipt) == proof['terminal_receipt_sha256'] and
                driver.sha256(stdout) == proof['stdout_sha256'] and driver.sha256(stderr) == proof['stderr_sha256'],
                'interactive image evidence changed after validation')
        receipt = interactive.parse(raw_receipt)
        require(receipt['mutation'] is commands.mutation_for(args), 'interactive mutation classification differs')
    require(not stderr, 'image command diagnostics')
    return SimpleNamespace(index=index, stdout=stdout, stderr=stderr, started_unix_ns=receipt['started_unix_ns'],
                           finished_unix_ns=receipt['started_unix_ns'] + receipt['elapsed_ns'])


class _Replay:
    def __init__(self, output, inputs, environment):
        self.output, self.inputs, self.env = Path(output), inputs, dict(environment)
        self.count = 0
        self.expected_files = set()
        self.previous_end = 0

    def _time(self, start, end):
        require(type(start) is int and type(end) is int and self.previous_end <= start <= end and start > 0,
                'image command clock order differs')
        self.previous_end = end

    def command(self, args, *, plan=None):
        index = self.count + 1
        require(index <= MAX_COMMANDS, 'image replay command bound')
        result = _read_command(self.output, self.inputs, self.env, index, args, plan)
        self._time(result.started_unix_ns, result.finished_unix_ns)
        self.count = index
        self._files(index, plan is not None)
        return result

    def _files(self, index, interactive_command=False):
        suffixes = ['.json', '.intent.json', '.stdout', '.stderr']
        if interactive_command:
            suffixes += ['.interaction-plan.json']
        self.expected_files.update('command-%05d%s' % (index, suffix) for suffix in suffixes)

    def guard(self):
        index = self.count + 1
        require(index + 1 <= MAX_COMMANDS, 'image replay guard bound')
        proof = commands.validate_guard(self.output, self.inputs, index, index + 1)
        for command in proof['commands']:
            row = command['receipt']
            self._time(row['started_unix_ns'], row['started_unix_ns'] + row['elapsed_ns'])
        self._files(index)
        self._files(index + 1)
        self.count += 2

    def finish(self):
        actual = {path.name for path in self.output.iterdir() if path.name.startswith('command-')}
        require(actual == self.expected_files, 'extra or missing image command evidence')


class _Program:
    def __init__(self, transport):
        self.transport = transport
        self.refs = references(transport.inputs)
        self.baseline = None
        self.workload_complete = self.cleanup_complete = False
        self.saved = self.first_save = self.second_save = None
        self._owned_content = {}

    def mutate(self, args, *, plan=None):
        self.transport.guard()
        value = self.transport.command(args, plan=plan)
        self.transport.guard()
        return value

    def inventory(self):
        self.transport.guard()
        ids = image_ids(self.transport.command(['image', 'ls', '--all', '--quiet', '--no-trunc']).stdout)
        rows = {}
        for image in ids:
            parsed = commands.decode(self.transport.command(['image', 'inspect', image]).stdout)
            require(type(parsed) is list and len(parsed) == 1, 'exact image inspect response')
            row = projection(parsed[0])
            require(row['Id'] == image, 'listed and inspected image differ')
            rows[image] = row
        require(len(canonical(rows)) <= fixture.LIMIT, 'image baseline aggregate bound')
        self.transport.guard()
        return rows

    def expected_inventory(self, roles):
        rows = self.inventory()
        expected_ids = set(self.baseline) | {fixture.fixture(role)['manifest_digest'] for role in roles}
        require(set(rows) == expected_ids, 'unexpected/missing image ID or unrelated inventory changed')
        require(all(canonical(rows[key]) == canonical(value) for key, value in self.baseline.items()),
                'unrelated baseline image changed')
        for role, refs in roles.items():
            expected = owned_image(rows[fixture.fixture(role)['manifest_digest']], role, refs)
            content = canonical({key: value for key, value in expected.items() if key not in ('RepoTags', 'RepoDigests')})
            require(role not in self._owned_content or self._owned_content[role] == content,
                    'owned image configuration changed across lifecycle')
            self._owned_content[role] = content
            for reference in refs:
                raw = self.transport.command(['image', 'inspect', '--platform', 'linux/arm64', reference]).stdout
                inspected = commands.decode(raw)
                require(type(inspected) is list and len(inspected) == 1 and
                        canonical(projection(inspected[0])) == canonical(expected),
                        'platform-selected reference differs from inventoried image')
        self.transport.guard()
        return rows

    def load(self, role, reference, raw):
        validate_archive(raw, role, reference)
        result = self.mutate(['image', 'load', '--platform', 'linux/arm64'], plan=io_plan(raw))
        # Pinned Moby image_exporter.go normalizes this status with FamiliarString.
        require(result.stdout == ('Loaded image: ' + fixture.familiar_reference(reference) + '\n').encode(),
                'image load acknowledgment differs')

    def save(self, reference):
        self.transport.guard()
        result = self.transport.command(['image', 'save', '--platform', 'linux/arm64', reference], plan=io_plan())
        proof = validate_archive(result.stdout, 'subject', reference)
        self.transport.guard()
        return result.stdout, proof

    def remove(self, role, reference, *, last_reference):
        require(type(last_reference) is bool, 'source-selected removal phase required')
        result = self.mutate(['image', 'rm', '--no-prune', reference])
        expected = fixture.fixture(role)
        familiar = fixture.familiar_reference(reference)
        untagged = ('Untagged: ' + familiar).encode()
        deleted = ('Deleted: ' + expected['manifest_digest']).encode()
        digest_reference = ('Untagged: ' + familiar.rsplit(':', 1)[0] + '@' + expected['manifest_digest']).encode()
        lines = result.stdout.splitlines()
        allowed = {untagged, digest_reference, deleted} if last_reference else {untagged}
        require(0 < len(lines) <= 3 and result.stdout.endswith(b'\n') and len(set(lines)) == len(lines) and
                all(line in allowed for line in lines) and untagged in lines and
                ((lines[-1] == deleted) if last_reference else lines == [untagged]),
                'image removal acknowledgment differs')

    def exercise(self):
        require(self.baseline is None, 'image recipe cannot be retried')
        self.baseline = self.inventory()
        require(len(self.baseline) <= MAX_IMAGES, 'initial image baseline count')
        ids = {fixture.fixture(role)['manifest_digest'] for role in fixture.ROLES}
        reserved = set(self.refs.values()) | {fixture.familiar_reference(ref) for ref in self.refs.values()}
        require(not ids.intersection(self.baseline) and not any(reserved.intersection(row['RepoTags'])
                    for row in self.baseline.values()), 'seed image ID/reference already exists')
        self.load('subject', self.refs['source'], fixture.archive('subject', self.refs['source']))
        self.expected_inventory({'subject': [self.refs['source']]})
        self.load('decoy', self.refs['decoy'], fixture.archive('decoy', self.refs['decoy']))
        before_tag = self.expected_inventory({'subject': [self.refs['source']], 'decoy': [self.refs['decoy']]})
        tagged = self.mutate(['tag', self.refs['source'], self.refs['alias']])
        require(not tagged.stdout, 'tag emitted unexpected output')
        self.expected_inventory({'subject': [self.refs['source'], self.refs['alias']], 'decoy': [self.refs['decoy']]})
        self.saved, self.first_save = self.save(self.refs['alias'])
        self.remove('subject', self.refs['alias'], last_reference=False)
        after_alias = self.expected_inventory({'subject': [self.refs['source']], 'decoy': [self.refs['decoy']]})
        require(canonical(before_tag) == canonical(after_alias), 'alias removal changed source or decoy')
        self.remove('subject', self.refs['source'], last_reference=True)
        decoy_only = self.expected_inventory({'decoy': [self.refs['decoy']]})
        self.load('subject', self.refs['alias'], self.saved)
        self.expected_inventory({'subject': [self.refs['alias']], 'decoy': [self.refs['decoy']]})
        _, self.second_save = self.save(self.refs['alias'])
        # Exporter TAR headers/padding need not be byte-identical. Each complete
        # archive independently verifies the same source-pinned content graph.
        self.remove('subject', self.refs['alias'], last_reference=True)
        after_reload = self.expected_inventory({'decoy': [self.refs['decoy']]})
        require(canonical(decoy_only) == canonical(after_reload), 'round trip changed decoy or baseline')
        self.workload_complete = True
        return self.proof()

    def cleanup(self):
        require(self.workload_complete and not self.cleanup_complete, 'semantic success required before decoy cleanup')
        self.expected_inventory({'decoy': [self.refs['decoy']]})
        self.remove('decoy', self.refs['decoy'], last_reference=True)
        self.expected_inventory({})
        self.cleanup_complete = True
        return self.proof()

    def proof(self):
        return {'schema_version': 1, 'scope': 'DEV_owned_image_roundtrip_not_physical_or_full_Docker_certification',
                'references': dict(self.refs), 'baseline': self.baseline, 'command_count': self.transport.count,
                'first_save': self.first_save, 'second_save': self.second_save,
                'workload_complete': self.workload_complete, 'cleanup_complete': self.cleanup_complete,
                'subject_absent': self.workload_complete, 'decoy_retained': self.workload_complete and not self.cleanup_complete,
                'full_baseline_restored': self.cleanup_complete, 'physical_execution_certified': False}


class ImageRoundTrip(_Program):
    """Live wrapper, retaining the caller's registered Driver on any failure."""
    def __init__(self, item):
        self.item = item
        self._exercise_verified = False
        self._exercise_proof = None
        super().__init__(_Live(item))

    def exercise(self):
        proof = super().exercise()
        repeated = replay(self.item.output, self.item.inputs.raw, environment=self.item.env)
        require(canonical(proof) == canonical(repeated), 'image workload raw replay differs')
        self._exercise_verified = True
        self._exercise_proof = canonical(proof)
        return proof

    def cleanup(self):
        require(self._exercise_verified, 'independent workload replay required before decoy cleanup')
        require(canonical(replay(self.item.output, self.item.inputs.raw, environment=self.item.env)) == self._exercise_proof,
                'original image workload evidence changed before cleanup')
        proof = super().cleanup()
        repeated = replay(self.item.output, self.item.inputs.raw, environment=self.item.env, cleanup=True)
        require(canonical(proof) == canonical(repeated), 'image cleanup raw replay differs')
        return proof


def replay(output, inputs, *, environment, cleanup=False):
    require(type(cleanup) is bool, 'explicit replay cleanup phase required')
    transport = _Replay(output, inputs, environment)
    selected = _Program(transport)
    result = selected.exercise()
    if cleanup:
        result = selected.cleanup()
    transport.finish()
    return result
