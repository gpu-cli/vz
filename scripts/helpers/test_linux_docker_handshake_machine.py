"""Offline handshake-adapter tests; no Docker, vz, VM or network dispatch."""
import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_handshake_machine as subject

ENGINE = 'engine-1'


def client(api_version, context='owned-machine'):
    return {'Version': '29.4.0', 'ApiVersion': api_version,
            'DefaultAPIVersion': '1.54', 'GitCommit': '9d7ad9f', 'GoVersion': 'go1.25.9', 'Os': 'darwin',
            'Arch': 'arm64', 'BuildTime': 'Mon Jan  1 00:00:00 2026', 'Context': context}


def server():
    details = {'ApiVersion': '1.55', 'Arch': 'arm64', 'BuildTime': 'x', 'Experimental': 'false', 'GitCommit': '6a43e3d',
               'GoVersion': 'go1.25.9', 'KernelVersion': '6.12.0', 'MinAPIVersion': '1.40', 'Os': 'linux'}
    return {'Platform': {'Name': 'Docker Engine - Community'},
            'Components': [{'Name': 'Engine', 'Version': '29.7.2', 'Details': details},
                           {'Name': 'containerd', 'Version': 'v2.1.0', 'Details': {'GitCommit': 'abc'}},
                           {'Name': 'youki', 'Version': ': 0.7.0', 'Details': {'GitCommit': '0.7.0-94ba653+vz-patches'}},
                           {'Name': 'docker-init', 'Version': '0.19.0', 'Details': {'GitCommit': 'ghi'}}],
            'Version': '29.7.2', 'ApiVersion': '1.55', 'MinAPIVersion': '1.40', 'GitCommit': '6a43e3d',
            'GoVersion': 'go1.25.9', 'Os': 'linux', 'Arch': 'arm64', 'KernelVersion': '6.12.0', 'BuildTime': 'x'}


def version_json(api_version, with_server=True):
    return json.dumps({'Client': client(api_version), 'Server': server() if with_server else None}).encode()


def info_json():
    return json.dumps({'ID': ENGINE, 'ServerVersion': '29.7.2', 'OSType': 'linux', 'Architecture': 'aarch64',
                       'DefaultRuntime': 'youki', 'CgroupVersion': '2', 'Warnings': None,
                       'Runtimes': {'youki': {'path': '/mnt/linux-bin/youki'}, 'runc': {'path': 'runc'},
                                    'io.containerd.runc.v2': {'path': 'runc'}}}).encode()


def flags_stream(args=('/usr/bin/dockerd', '--host', 'unix:///run/docker.sock', '--default-runtime', 'youki'), env=(), count=1):
    lines = ['VZ_DOCKERD_FLAGS_V1']
    for _ in range(count):
        lines += ['PID=412', *('ARG=' + a for a in args), *('ENV=' + e for e in env)]
    lines += ['COUNT=' + str(count), 'VZ_DOCKERD_FLAGS_END', '']
    return '\n'.join(lines).encode()


class Machine(unittest.TestCase):
    @contextmanager
    def case(self, *, outputs=None, probes=None, flags=None):
        with tempfile.TemporaryDirectory(prefix='vz-handshake-unit-') as temporary:
            root = Path(temporary).resolve()
            evidence = root / 'evidence'
            evidence.mkdir(mode=0o700)
            events = []
            scope = {'project_id': 'project-1', 'environment_id': 'environment-1', 'machine_id': 'machine-1',
                     'machine_incarnation': 'incarnation-1', 'runtime_identity': 'runtime-1',
                     'docker_context': 'owned-machine', 'docker_endpoint': 'unix:///private/owned/docker.sock',
                     'engine_id': ENGINE}
            descriptor = {'owner': {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
                          'name': scope['docker_context'], 'endpoint': scope['docker_endpoint'], 'config_dir': str(root / 'cfg'),
                          'engine_id': scope['engine_id'], 'incarnation_id': scope['machine_incarnation'],
                          'incarnation_generation': 1}
            proof = {'receipt_path': '/original/runtime-proof.json', 'receipt_sha256': 'a' * 64}
            images = {'base': {'id': 'sha256:' + 'b' * 64}, 'compose': {'id': 'sha256:' + 'c' * 64}}
            outputs = dict(outputs or {})
            outputs.setdefault('handshake-version', (version_json('1.54'), b'', 0))
            outputs.setdefault('handshake-info', (info_json(), b'', 0))
            probes = dict(probes or {})
            for version in ('1.39', '1.56'):
                probes.setdefault(version, (version_json(version, with_server=False),
                                            (subject.DAEMON_ERROR_PREFIX + subject.REJECTED[version] + '\n').encode(), 1))
            for version in ('1.40', '1.55'):
                probes.setdefault(version, (version_json(version), b'', 0))
            flags = flags_stream() if flags is None else flags
            env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'VZ_DOCKER_CONFIG': str(root / 'docker')}
            harness = SimpleNamespace(evidence=evidence, descriptors=[copy.deepcopy(descriptor)], effects_uncertain=False,
                env=dict(env), root=root, cli=root / 'install/bin/vz',
                info={'clients': {'docker': {'canonical': '/owned/docker'}}, 'inputs': {name: subject.driver.sha256(
                    subject.driver.regular(Path(name), subject.LIMIT)) for name in subject.required_source_paths()}},
                monitor=SimpleNamespace(check=Mock(side_effect=lambda: events.append('monitor'))),
                mutate=Mock(side_effect=AssertionError('handshake must never mutate')))
            def docker(label, given, args, **kwargs):
                self.assertEqual(given, descriptor)
                events.append(label)
                return outputs[label]
            def command(label, argv, cwd=None, **kwargs):
                events.append(label)
                self.assertEqual(argv[:3], [harness.cli, 'exec', '--environment'])
                self.assertEqual(argv[argv.index('--machine') + 1], 'machine-1')
                self.assertEqual(argv[-4:-1], ['/bin/busybox', 'sh', '-c'])
                self.assertEqual(argv[-1], subject.FLAG_SCRIPT)
                self.assertEqual(cwd, root / 'project')
                self.assertFalse(kwargs['success'])
                return flags, b'', 0
            harness.docker, harness.command = Mock(side_effect=docker), Mock(side_effect=command)
            recorders = []
            class Recorder:
                def __init__(self, directory, environment):
                    self.root, self.env, self.receipts = directory, dict(environment), []
                    recorders.append(self)
                def run(self, label, argv, *, executable, cwd, timeout, success):
                    events.append(label)
                    self_case.assertTrue(self.root.is_dir())
                    self_case.assertFalse(success)
                    self_case.assertEqual(executable, '/owned/docker')
                    self_case.assertEqual(cwd, root)
                    version = self.env['DOCKER_API_VERSION']
                    self_case.assertEqual(argv, ['docker', '--config', descriptor['config_dir'], '--context', 'owned-machine',
                                                 'version', '--format', '{{json .}}'])
                    self.receipts.append({'capture_complete': True, 'effects_uncertain': False})
                    return probes[version]
            self_case = self
            binding = {'project_path': str(root / 'project'), 'project_definition_sha256': 'd' * 64,
                       'retained_topology_sha256': 'e' * 64}
            (root / 'project').mkdir()
            with ExitStack() as stack:
                stack.enter_context(patch.object(subject.startup, 'Recorder', Recorder))
                stack.enter_context(patch.object(subject.cgroup, 'project_binding', Mock(return_value=dict(binding))))
                yield SimpleNamespace(harness=harness, scope=scope, proof=proof, images=images, descriptor=descriptor,
                    events=events, outputs=outputs, probes=probes, recorders=recorders, env=env, evidence=evidence,
                    invoke=lambda: subject.run_machine(harness, descriptor, scope, proof, images, 0))

    def test_full_handshake_records_proof_without_mutation_or_environment_leak(self):
        with self.case() as case:
            result = case.invoke()
            self.assertEqual(result['scope'], subject.SCOPE)
            self.assertEqual(result['negotiated']['client_api_version'], '1.54')
            self.assertEqual(result['negotiated']['server_api_version'], '1.55')
            self.assertEqual(result['negotiated']['server_min_api_version'], '1.40')
            self.assertEqual(result['negotiated']['component_names'], ['Engine', 'containerd', 'youki', 'docker-init'])
            self.assertEqual(result['info']['default_runtime'], 'youki')
            self.assertTrue(result['daemon_flags']['min_api_override_absent'])
            self.assertEqual([row['outcome'] for row in result['api_version_probes']],
                             ['rejected', 'rejected', 'accepted', 'accepted'])
            self.assertEqual([row['api_version'] for row in result['api_version_probes']], ['1.39', '1.56', '1.40', '1.55'])
            self.assertFalse(result['release_certified'])
            self.assertFalse(result['docker_parity_certified'])
            self.assertEqual(result['mutations'], 0)
            case.harness.mutate.assert_not_called()
            self.assertEqual(case.harness.env, case.env)
            self.assertEqual([r.env['DOCKER_API_VERSION'] for r in case.recorders], ['1.39', '1.56', '1.40', '1.55'])
            for recorder in case.recorders:
                self.assertEqual({k: v for k, v in recorder.env.items() if k != 'DOCKER_API_VERSION'}, case.env)
                self.assertEqual(recorder.root.parent, case.evidence / 'handshake-machine-0')
            for raw in (result['negotiated']['stdout_sha256'], result['info']['stdout_sha256'],
                        result['daemon_flags']['stdout_sha256']):
                self.assertRegex(raw, '[0-9a-f]{64}')
            self.assertEqual(result['negotiated']['stdout_sha256'], subject.sha256(case.outputs['handshake-version'][0]))
            retained = subject.parse((case.evidence / 'handshake-machine-0/machine-handshake-validation.json').read_bytes())
            self.assertEqual(retained, result)
            self.assertEqual(case.events[:2], ['monitor', 'handshake-version'])

    def test_manifest_reconciles_with_upstream_pins(self):
        manifest = subject.manifest_expectations()
        self.assertEqual(manifest['candidate_versions']['engine_api']['minimum'], '1.40')
        self.assertEqual(manifest['candidate_versions']['engine_api']['maximum'], '1.55')
        self.assertEqual(subject.NEGOTIATED, min(subject.CLIENT['MaxAPIVersion'], subject.SERVER['MaxAPIVersion']))
        raw = json.loads(subject.MANIFEST.read_bytes())
        for change in ('engine_api.maximum', 'engine_api.minimum', 'docker_cli.build', 'docker_engine.maximum', 'scenario'):
            forged = copy.deepcopy(raw)
            if change == 'scenario':
                next(s for s in forged['scenarios'] if s['id'] == 'docker.engine.api_negotiation')['expected']['api_1_39'] = 'accept'
            else:
                group, key = change.split('.')
                forged['candidate_versions'][group][key] = '9.99' if key != 'build' else 'ffffff'
            with tempfile.NamedTemporaryFile('wb', suffix='.json', delete=False) as handle:
                handle.write(json.dumps(forged).encode())
            self.addCleanup(Path(handle.name).unlink)
            with self.subTest(change=change), patch.object(subject, 'MANIFEST', Path(handle.name)):
                with self.assertRaises(ValueError):
                    subject.manifest_expectations()

    def test_wrong_version_info_or_flag_outputs_fail_closed(self):
        def version(mutate):
            value = json.loads(version_json('1.54'))
            mutate(value)
            return json.dumps(value).encode()
        cases = {
            'server-version': version(lambda v: v['Server'].__setitem__('Version', '29.7.1')),
            'server-arch': version(lambda v: v['Server'].__setitem__('Arch', 'amd64')),
            'client-version': version(lambda v: v['Client'].__setitem__('Version', '29.3.0')),
            'client-os': version(lambda v: v['Client'].__setitem__('Os', 'linux')),
            'client-context': version(lambda v: v['Client'].__setitem__('Context', 'default')),
            'negotiated-not-min': version(lambda v: v['Client'].__setitem__('ApiVersion', '1.55')),
            'client-default': version(lambda v: v['Client'].__setitem__('DefaultAPIVersion', '1.55')),
            'server-min-override': version(lambda v: v['Server'].__setitem__('MinAPIVersion', '1.24')),
            'server-max': version(lambda v: v['Server'].__setitem__('ApiVersion', '1.54')),
            'engine-details': version(lambda v: v['Server']['Components'][0]['Details'].__setitem__('GitCommit', 'ffffff')),
            'missing-component': version(lambda v: v['Server']['Components'].pop(1)),
            'runc-component': version(lambda v: v['Server']['Components'].append({'Name': 'runc', 'Version': '1.3.0'})),
            'duplicate-field': b'{"Client": {}, "Client": {}, "Server": null}',
            'not-json': b'Client: Docker Engine\n'}
        for name, raw in cases.items():
            with self.subTest(name=name), self.case(outputs={'handshake-version': (raw, b'', 0)}) as case:
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertNotIn('handshake-info', case.events)
                self.assertEqual(case.recorders, [])
                self.assertFalse((case.evidence / 'handshake-machine-0/machine-handshake-validation.json').exists())
        with self.case(outputs={'handshake-version': (version_json('1.54'), b'warning\n', 0)}) as case:
            with self.assertRaises(ValueError):
                case.invoke()
        for change in ({'DefaultRuntime': 'runc'}, {'ID': 'foreign'}, {'ServerVersion': '29.7.1'}, {'CgroupVersion': '1'},
                       {'Runtimes': {'runc': {}}}):
            info = json.loads(info_json()) | change
            with self.subTest(change=change), self.case(outputs={'handshake-info': (json.dumps(info).encode(), b'', 0)}) as case:
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertEqual(case.recorders, [])
        for name, flags in (('flag', flags_stream(args=('/usr/bin/dockerd', '--min-api-version=1.24'))),
                            ('flag-split', flags_stream(args=('/usr/bin/dockerd', '--min-api-version', '1.24'))),
                            ('env', flags_stream(env=('DOCKER_MIN_API_VERSION=1.24',))),
                            ('two-daemons', flags_stream(count=2)), ('none', flags_stream(count=0)),
                            ('truncated', flags_stream()[:-10]), ('foreign-argv0', flags_stream(args=('/usr/bin/containerd',)))):
            with self.subTest(name=name), self.case(flags=flags) as case:
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertEqual(case.recorders, [])

    def test_override_probe_outcomes_must_match_pinned_daemon_text(self):
        rejected = (subject.DAEMON_ERROR_PREFIX + subject.REJECTED['1.39'] + '\n').encode()
        cases = {
            '1.39-accepted': ('1.39', (version_json('1.39'), b'', 0)),
            '1.39-wrong-text': ('1.39', (version_json('1.39', False), rejected.replace(b'1.40', b'1.44'), 1)),
            '1.39-no-prefix': ('1.39', (version_json('1.39', False), (subject.REJECTED['1.39'] + '\n').encode(), 1)),
            '1.39-server-present': ('1.39', (version_json('1.39'), rejected, 1)),
            '1.56-old-text': ('1.56', (version_json('1.56', False), rejected, 1)),
            '1.40-rejected': ('1.40', (version_json('1.40', False), rejected, 1)),
            '1.55-stderr': ('1.55', (version_json('1.55'), b'warning\n', 0)),
            '1.55-negotiated-instead': ('1.55', (version_json('1.54'), b'', 0)),
            '1.40-server-drift': ('1.40', (version_json('1.40').replace(b'"Version": "29.7.2"', b'"Version": "29.7.3"'), b'', 0))}
        for name, (version, output) in cases.items():
            with self.subTest(name=name), self.case(probes={version: output}) as case:
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertEqual(case.harness.env, case.env)
                self.assertFalse((case.evidence / 'handshake-machine-0/machine-handshake-validation.json').exists())

    def test_harness_environment_with_override_is_rejected(self):
        with self.case() as case:
            case.harness.env['DOCKER_API_VERSION'] = '1.50'
            with self.assertRaisesRegex(ValueError, 'already overrides'):
                case.invoke()

    def test_foreign_scope_descriptor_proof_index_or_evidence_rejected_before_commands(self):
        for field in ('machine_id', 'machine_incarnation', 'docker_context', 'docker_endpoint', 'engine_id'):
            with self.subTest(field=field), self.case() as case:
                case.scope[field] = 'foreign'
                with self.assertRaises(ValueError):
                    case.invoke()
                self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.descriptors = []
            with self.assertRaises(ValueError):
                case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.proof.clear()
            with self.assertRaises(ValueError):
                case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            with self.assertRaises(ValueError):
                subject.run_machine(case.harness, case.descriptor, case.scope, case.proof, case.images, True)
            self.assertEqual(case.events, [])
        with self.case() as case:
            (case.evidence / 'handshake-machine-0').mkdir()
            with self.assertRaises(ValueError):
                case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.effects_uncertain = True
            with self.assertRaises(ValueError):
                case.invoke()
            self.assertEqual(case.events, [])

    def test_missing_or_changed_source_pin_prevents_dispatch(self):
        for missing in (False, True):
            with self.subTest(missing=missing), self.case() as case:
                path = subject.required_source_paths()[0]
                if missing:
                    del case.harness.info['inputs'][path]
                else:
                    case.harness.info['inputs'][path] = '0' * 64
                with self.assertRaises((ValueError, KeyError)):
                    case.invoke()
                self.assertEqual(case.events, [])
        self.assertIn(str(subject.MANIFEST), subject.required_source_paths())

    def test_inputs_are_not_mutated_and_result_documents_are_resealed(self):
        with self.case() as case:
            snapshot = copy.deepcopy((case.descriptor, case.scope, case.proof, case.images))
            case.invoke()
            self.assertEqual((case.descriptor, case.scope, case.proof, case.images), snapshot)
        original = subject.startup.document
        def tamper(path, value):
            original(path, dict(value, forged=True) if path.name == 'machine-handshake-validation.json' else value)
        with self.case() as case, patch.object(subject.startup, 'document', side_effect=tamper):
            with self.assertRaisesRegex(ValueError, 'retained handshake result differs'):
                case.invoke()


if __name__ == '__main__':
    unittest.main()
