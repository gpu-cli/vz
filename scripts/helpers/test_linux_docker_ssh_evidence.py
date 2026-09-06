"""Synthetic independently resealed SSH evidence tests; no Docker or SSH server."""
import base64
import copy
import json
from pathlib import Path
import shutil
import unittest

import linux_docker_ssh_evidence as evidence
import test_linux_docker_artifact_evidence as artifacts
from test_linux_docker_build_evidence import SyntheticBuilder


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


class SSHEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        artifacts.ArtifactEvidenceTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        artifacts.ArtifactEvidenceTests.tearDownClass()

    def setUp(self):
        self.raw = artifacts.ArtifactEvidenceTests()
        self.raw.setUp()
        self.addCleanup(self.raw.doCleanups)
        self.directory, self.inputs = self.raw.directory, self.raw.inputs
        self.fixture = self.raw.flow / 'ssh-fixture'
        self.fixture.mkdir(mode=0o700)
        original = Path(__file__).resolve().parents[2] / 'tests/fixtures/vz-0.4/docker-ssh'
        for path in original.iterdir():
            if path.is_file():
                shutil.copyfile(path, self.fixture / path.name)
        # Synthetic package closure: parser tests do not claim authentic package admission.
        self.packages = [(f'package-{i}.deb', ('fixture-package-' + str(i)).encode()) for i in range(8)]
        pins = {'schema_version': 1, 'dpkg_deb_sha256': 'a' * 64, 'packages': [
            {'filename': name, 'package': name[:-4], 'version': '1', 'architecture': 'arm64',
             'sha256': evidence.sha(data), 'size': len(data)} for name, data in self.packages]}
        (self.fixture / 'package-pins.json').write_bytes(canonical(pins))
        self.context = self.raw.flow / 'ssh-context'
        shutil.copytree(self.fixture, self.context)
        (self.context / 'inputs').mkdir(mode=0o700)
        (self.context / 'packages').mkdir(mode=0o700)
        (self.context / 'packages/manifest.json').write_bytes(canonical(pins))
        for name, data in self.packages:
            (self.context / 'packages' / name).write_bytes(data)
        self.wire = b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20' + b'a' * 32
        self.request = {'schema_version': 1, 'token': 'vzssh-' + 'a' * 24, 'host': '172.18.0.2', 'port': 2222,
                        'host_key_fingerprint': 'SHA256:' + base64.b64encode(bytes.fromhex(evidence.sha(self.wire))).decode().rstrip('=')}
        self.make('declared')

    def make(self, case):
        wire = self.wire if case != 'wrong_host' else self.wire[:19] + b'b' * 32
        hosts = b'[172.18.0.2]:2222 ssh-ed25519 ' + base64.b64encode(wire) + b'\n'
        (self.context / 'inputs/known_hosts').write_bytes(hosts)
        (self.context / 'inputs/request.json').write_bytes(canonical(self.request))
        self.op = {'schema_version': 1, 'case': case, 'run_id': self.inputs['run_id'],
                   'ssh_fixture': str(self.fixture), 'ssh_fixture_sha256': evidence.fixture_digest(self.fixture),
                   'build_context': str(self.context), 'build_context_sha256': evidence.fixture_digest(self.context),
                   'request': self.request, 'known_hosts_sha256': evidence.sha(hosts),
                   'agent_socket': None if case == 'provider_omitted' else str(self.raw.flow / 'agent.sock'),
                   'output': str(self.directory / 'oci'),
                   'cache_output': str(self.directory / 'cache') if case == 'declared' else None,
                   'build_argv': []}
        self.op['build_argv'] = evidence.build_argv(self.inputs, self.op)
        self.persist()
        self.raw.write('artifact-validation.json', {'oci': self.raw.image if case == 'declared' else None,
                                                   'cache': self.raw.cache if case == 'declared' else None})
        for index, seconds in enumerate((0, .1, .2, .3, .4, 8.5, 8.6, 8.7, 8.8), 1):
            self.raw.change(index, lambda row, i=index, s=seconds: row.update(
                started_unix_ns=evidence.progress_ns(SyntheticBuilder.stamp(s)),
                elapsed_ns=8_000_000_000 if i == 5 else 10_000_000,
                exit_code=int(i == 5 and case != 'declared'), effects_uncertain=i == 5 and case != 'declared'), True)
        self.raw.change(5, lambda row: row.update(argv=row['argv'][:5] + self.op['build_argv']), True)
        self.batch, self.footer = self.progress(case)
        self.seal_progress()

    def persist(self):
        self.raw.write('operation.intent.json', self.op)
        self.raw.write('operation.json', self.op)

    def result(self, case):
        stdout = ('vz-ssh-response:' + self.request['token'] + '\n').encode() if case == 'declared' else b''
        stderr = b'' if case == 'declared' else (
            b'vzssh@172.18.0.2: Permission denied (publickey).\r\n' if case == 'undeclared'
            else evidence.hostkey_diagnostic(self.request))
        return {'schema_version': 1, 'type': 'ssh_result', 'token': self.request['token'],
                'mode': 'undeclared' if case == 'undeclared' else 'mounted', 'host': self.request['host'], 'port': 2222,
                'started_unix_ns': evidence.progress_ns(SyntheticBuilder.stamp(4.2)),
                'completed_unix_ns': evidence.progress_ns(SyntheticBuilder.stamp(4.8)),
                'ssh_exit_code': 0 if case == 'declared' else 255,
                'outcome': {'declared': 'authenticated', 'undeclared': 'publickey_denied', 'wrong_host': 'hostkey_denied'}[case],
                'stdout_sha256': evidence.sha(stdout), 'stderr_sha256': evidence.sha(stderr),
                'stdout_bytes': len(stdout), 'stderr_bytes': len(stderr)}

    def progress(self, case):
        total = 6 if case == 'undeclared' else 7
        prefix = lambda n: '[build ' + str(n) + '/' + str(total) + '] '
        mode = 'undeclared' if case == 'undeclared' else 'mounted'
        run = 'RUN ' + ('' if case == 'undeclared' else '--mount=type=ssh,id=fixture,required=true,target=/run/vz-build-ssh-agent ') + 'python3 /fixture/ssh_probe.py ' + mode
        names = [prefix(1) + 'FROM ' + self.inputs['images']['base']['reference'], '[internal] load build context',
                 prefix(2) + 'COPY packages/ /fixture/packages/',
                 prefix(3) + 'COPY packages.py package-pins.json ssh_probe.py /fixture/',
                 prefix(4) + 'RUN --network=none python3 /fixture/packages.py',
                 prefix(5) + 'COPY inputs/ /fixture/inputs/', prefix(6) + run]
        links = ([], [], [0, 1], [2, 1], [3], [4, 1], [5], [6], [7], [], [])
        times = [(1, 2), (1, 2), (2, 2.5), (2.5, 3), (3, 3.5), (3.5, 4), (4, 5),
                 (5, 5.5), (5.5, 6), (6, 7), (7, 8)]
        if case == 'declared':
            names += [prefix(7) + 'RUN python3 /fixture/ssh_probe.py absent',
                      '[output 1/1] COPY --from=build /out/ssh.txt /ssh.txt', evidence.OCI_EXPORT, evidence.CACHE_EXPORT]
        ids = ['sha256:' + evidence.sha(n.encode()) for n in names]
        vertices = [{'digest': identity, 'name': name, 'inputs': [ids[i] for i in links[number]],
                     'started': SyntheticBuilder.stamp(times[number][0]), 'completed': SyntheticBuilder.stamp(times[number][1]),
                     'cached': False} for number, (identity, name) in enumerate(zip(ids, names))]
        manifest = (self.fixture / 'package-pins.json').read_bytes()
        packages = {'schema_version': 1, 'type': 'openssh_packages_extracted', 'package_pins_sha256': evidence.sha(manifest),
                    'packages': [r['package'] for r in json.loads(manifest)['packages']], 'maintainer_scripts_executed': False}
        log = lambda index, value, time: {'vertex': ids[index], 'stream': 1, 'data': base64.b64encode(canonical(value)).decode(),
                                          'timestamp': SyntheticBuilder.stamp(time)}
        logs = [log(4, packages, 3.4)]
        if case != 'provider_omitted':
            logs.append(log(6, self.result(case), 4.9))
        if case == 'declared':
            logs.append(log(7, {'schema_version': 1, 'type': 'ssh_mount_absent', 'token': self.request['token'],
                               'agent_path_absent': True, 'agent_environment_absent': True,
                               'unix_ns': evidence.progress_ns(SyntheticBuilder.stamp(5.2))}, 5.3))
        batch = {'vertexes': vertices, 'logs': logs}
        if case == 'declared':
            # Reuse only independently tested exporter status fixtures, not SSH graph construction.
            exports = json.loads(self.raw.progress(False, False, True))
            batch['statuses'] = exports['statuses']
        footer = b''
        if case != 'declared':
            error = evidence.MISSING if case == 'provider_omitted' else (
                'process "/bin/sh -c python3 /fixture/ssh_probe.py ' + mode + '" did not complete successfully: exit code: ' +
                ('41' if case == 'undeclared' else '42'))
            vertices[6]['error'] = error
            footer = ('ERROR: failed to build: failed to solve: ' + error + '\n').encode()
        return batch, footer

    def seal_progress(self):
        self.raw.stream(5, 'stderr', canonical(self.batch) + self.footer)
        if self.op['case'] != 'declared':
            self.raw.write('command-00005.acknowledgement.json', {'command_index': 5,
                'assertion': 'terminal BuildKit SSH fixture ' + self.op['case'] + ' denial',
                'terminal_receipt_sha256': evidence.sha((self.directory / 'command-00005.json').read_bytes()), 'effects_uncertain': False})

    def validate(self, **kwargs):
        return evidence.validate_operation(self.directory, self.inputs, self.op, **kwargs)

    def test_four_exact_cases(self):
        for case in sorted(evidence.CASES):
            with self.subTest(case=case):
                self.make(case)
                proof = self.validate()
                self.assertEqual(proof['case'], case)
                self.assertFalse(proof['compatibility_certified'])
                ack = self.directory / 'command-00005.acknowledgement.json'
                if ack.exists():
                    ack.unlink()

    def test_engine_repo_alias_normalizes_graph_names_without_digest_relaxation(self):
        pin = self.inputs['images']['base']['reference'].split('@sha256:')[1]
        reference = 'docker.io/library/python@sha256:' + pin
        self.inputs['images']['base']['reference'] = reference
        self.make('declared')
        raw = canonical(self.batch) + self.footer
        def replay(selected):
            return evidence.ssh_progress(raw, reference=selected, case='declared', request=self.request,
                package_manifest=(self.fixture / 'package-pins.json').read_bytes(),
                dockerfile=(self.fixture / 'Dockerfile.ssh').read_bytes(),
                guest_lower=evidence.progress_ns(SyntheticBuilder.stamp(0)),
                guest_upper=evidence.progress_ns(SyntheticBuilder.stamp(9)),
                host_lower=evidence.progress_ns(SyntheticBuilder.stamp(0)),
                host_upper=evidence.progress_ns(SyntheticBuilder.stamp(9)))
        expected = replay(reference)
        for name in ('python', 'library/python', 'docker.io/library/python'):
            with self.subTest(name=name):
                self.assertEqual(replay(name + '@sha256:' + pin), expected)
        for bad in ('python:latest', 'python@sha256:' + '0' * 64, 'other/python@sha256:' + pin):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                replay(bad)

    def test_missing_ack_preack_only(self):
        self.make('undeclared')
        (self.directory / 'command-00005.acknowledgement.json').unlink()
        self.assertEqual(self.validate(require_ack=False)['case'], 'undeclared')
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_preack_still_rejects_forged_denial(self):
        self.make('undeclared')
        self.batch['vertexes'][6]['error'] = 'connection refused'
        self.seal_progress()
        with self.assertRaises(evidence.Invalid):
            self.validate(require_ack=False)

    def test_vertex_adversaries(self):
        mutations = [lambda b: b['vertexes'][6].update(cached=True),
                     lambda b: b['vertexes'][6].update(inputs=[]),
                     lambda b: b['vertexes'][0].update(name='[build 1/7] FROM mutable:latest'),
                     lambda b: b['vertexes'].append(dict(b['vertexes'][6], digest='sha256:' + 'f' * 64)),
                     lambda b: b['vertexes'].append(dict(b['vertexes'][6])),
                     lambda b: b.update(warnings=[{'short': 'warning'}]),
                     lambda b: b['vertexes'][8].update(completed='2030-01-01T00:00:00Z')]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.make('declared'); mutation(self.batch); self.seal_progress()
                with self.assertRaises(evidence.Invalid):
                    self.validate()

    def test_transcript_adversaries(self):
        changes = [{'token': 'vzssh-' + 'b' * 24}, {'outcome': 'operational_error'}, {'ssh_exit_code': True},
                   {'stderr_sha256': 'f' * 64}, {'started_unix_ns': 1}, {'extra': False}]
        for change in changes:
            with self.subTest(change=change):
                self.make('declared'); value = self.result('declared'); value.update(change)
                self.batch['logs'][1]['data'] = base64.b64encode(canonical(value)).decode(); self.seal_progress()
                with self.assertRaises(evidence.Invalid):
                    self.validate()

    def test_provider_omission_never_executes_probe(self):
        self.make('provider_omitted')
        self.batch['logs'].append({'vertex': self.batch['vertexes'][6]['digest'], 'stream': 1,
                                  'timestamp': SyntheticBuilder.stamp(4.9), 'data': base64.b64encode(canonical(self.result('declared'))).decode()})
        self.seal_progress()
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_wrong_missing_provider_branch_rejected(self):
        self.make('provider_omitted')
        self.batch['vertexes'][6]['error'] = 'no SSH key "fixture" forwarded from the client'
        self.footer = b'ERROR: failed to build: failed to solve: no SSH key "fixture" forwarded from the client\n'
        self.seal_progress()
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_foreign_trailer_and_logs_rejected(self):
        for kind in ('footer', 'owner', 'stream', 'canary'):
            with self.subTest(kind=kind):
                self.make('undeclared')
                if kind == 'footer':
                    self.footer += b'connection refused\n'
                elif kind == 'owner':
                    self.batch['logs'][1]['vertex'] = self.batch['vertexes'][5]['digest']
                elif kind == 'stream':
                    self.batch['logs'][1]['stream'] = 2
                else:
                    self.batch['logs'][1]['data'] = base64.b64encode(b'private-canary-never-public').decode()
                self.seal_progress()
                with self.assertRaises((evidence.Invalid, ValueError)):
                    self.validate(secret_canaries=(b'private-canary-never-public',))

    def test_raw_receipt_and_argv_adversaries(self):
        changes = [lambda r: r.update(capture_complete=False), lambda r: r.update(executable='/wrong/docker'),
                   lambda r: r.update(argv=r['argv'] + ['--cache-from', 'type=local,src=/foreign']),
                   lambda r: r.update(environment=dict(r['environment'], DOCKER_HOST='unix:///foreign'))]
        for change in changes:
            with self.subTest(change=change):
                self.make('declared'); self.raw.change(5, change, True)
                with self.assertRaises(evidence.Invalid):
                    self.validate()

    def test_negative_exported_bytes_rejected(self):
        self.make('undeclared')
        (self.directory / 'oci/unexpected').write_bytes(b'output')
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_package_staged_bytes_and_source_drift(self):
        (self.context / 'packages/package-0.deb').write_bytes(b'replaced')
        self.op['build_context_sha256'] = evidence.fixture_digest(self.context); self.persist()
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_duplicate_json_and_extra_inventory_rejected(self):
        (self.directory / 'operation.json').write_bytes(b'{"schema_version":1,"schema_version":1}')
        with self.assertRaises(evidence.Invalid):
            self.validate()
        self.persist(); (self.directory / 'unrecorded').write_bytes(b'')
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_source_phases_may_repeat_without_operation_aliasing(self):
        base = self.batch['vertexes'][0]
        self.batch['vertexes'].insert(0, dict(base, started=SyntheticBuilder.stamp(.6), completed=SyntheticBuilder.stamp(.8)))
        self.batch['vertexes'].insert(2, dict(base))
        self.seal_progress()
        self.assertEqual(self.validate()['case'], 'declared')

    def test_inspection_diagnostic_is_not_ignored(self):
        self.raw.stream(3, 'stderr', b'unexpected inspection error\n')
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_private_canary_json_escape_rejected(self):
        self.batch['statuses'][0]['name'] = 'private-canary-never-public'
        raw = canonical(self.batch).replace(b'private-canary-never-public', b'\\u0070rivate-canary-never-public')
        self.raw.stream(5, 'stderr', raw)
        with self.assertRaises((evidence.Invalid, ValueError)):
            self.validate(secret_canaries=(b'private-canary-never-public',))

    def test_status_error_and_invalid_counter_rejected(self):
        for change in ({'error': 'connection refused'}, {'current': True}, {'completed': '2030-01-01T00:00:00Z'}):
            with self.subTest(change=change):
                self.make('declared'); self.batch['statuses'][0].update(change); self.seal_progress()
                with self.assertRaises(evidence.Invalid):
                    self.validate()

    def test_absence_and_package_execution_must_be_exact(self):
        for position, change in ((2, {'agent_path_absent': False}), (2, {'unix_ns': 1}),
                                 (0, {'maintainer_scripts_executed': True}), (0, {'package_pins_sha256': 'f' * 64})):
            with self.subTest(change=change):
                self.make('declared')
                value = json.loads(base64.b64decode(self.batch['logs'][position]['data']))
                value.update(change)
                self.batch['logs'][position]['data'] = base64.b64encode(canonical(value)).decode()
                self.seal_progress()
                with self.assertRaises(evidence.Invalid):
                    self.validate()

    def test_negative_cannot_execute_downstream_vertex(self):
        self.make('wrong_host')
        self.batch['vertexes'].append({'digest': 'sha256:' + 'f' * 64,
            'name': '[build 7/7] RUN python3 /fixture/ssh_probe.py absent',
            'inputs': [self.batch['vertexes'][6]['digest']], 'started': SyntheticBuilder.stamp(5),
            'completed': SyntheticBuilder.stamp(5.5)})
        self.seal_progress()
        with self.assertRaises(evidence.Invalid):
            self.validate()

    def test_known_hosts_contract_and_preack_existing_ack(self):
        self.make('wrong_host')
        self.raw.write('command-00005.acknowledgement.json', {'command_index': 5, 'effects_uncertain': False,
            'assertion': 'arbitrary failure', 'terminal_receipt_sha256': evidence.sha((self.directory / 'command-00005.json').read_bytes())})
        with self.assertRaises(evidence.Invalid):
            self.validate(require_ack=False)
        self.seal_progress()
        hosts = b'[172.18.0.2]:2222 ssh-ed25519 ' + base64.b64encode(self.wire) + b'\n'
        (self.context / 'inputs/known_hosts').write_bytes(hosts)
        self.op.update(known_hosts_sha256=evidence.sha(hosts), build_context_sha256=evidence.fixture_digest(self.context))
        self.persist()
        with self.assertRaises(evidence.Invalid):
            self.validate()


if __name__ == '__main__':
    unittest.main()
