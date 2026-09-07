"""Offline registry-suite wiring tests; no Docker, vz, VM or registry dispatch."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_e2e as gate
import linux_docker_registry_machine as subject

COMMON = [part for name in gate.startup.OPTIONS for part in ('--' + name, '/owned/value')]
REGISTRY = ['--registry-archive', '/owned/registry.tar', '--registry-layout', '/owned/layout']
FOREIGN = ('buildkit-archive', 'parallel-fixture', 'ssh-fixture', 'ssh-packages', 'ssh-gpgv', 'container-fixture', 'tmux')


def registry_record(archive_sha256='e' * 64):
    return {'schema_version': 1, 'scope': subject.SCOPE, 'pin': str(subject.PIN), 'layout': '/owned/layout',
            'layout_inventory_sha256': 'a' * 64, 'pins_sha256': 'b' * 64, 'reference': 'docker.io/library/registry:3.1.1',
            'manifest_digest': subject.MANIFEST_DIGEST, 'config_digest': 'sha256:' + 'c' * 64,
            'upstream_index_digest': 'sha256:' + 'd' * 64, 'archive_sha256': archive_sha256,
            'archive_bytes': 18847232, 'archive_members': 10, 'binary_sha256': subject.BINARY_SHA256,
            'binary_size': 50331832, 'binary_layer_digest': 'sha256:' + 'f' * 64, 'version': subject.VERSION,
            'go_version': subject.GO_VERSION, 'dependencies': {'cryptography': '50.0.1', 'bcrypt': '5.0.0'},
            'binary_executed': False, 'release_certified': False}


class ArgumentTests(unittest.TestCase):
    def test_registry_requires_both_inputs_and_only_for_registry(self):
        args = gate.arguments(['--suite', 'registry', *COMMON, *REGISTRY])
        self.assertEqual(args.suite, 'registry')
        self.assertTrue(args.run_id.startswith('registry-'))
        self.assertEqual((args.registry_archive, args.registry_layout), ('/owned/registry.tar', '/owned/layout'))
        for partial in (REGISTRY[:2], REGISTRY[2:], []):
            with self.subTest(partial=partial), self.assertRaisesRegex(ValueError, 'is required for the registry suite'):
                gate.arguments(['--suite', 'registry', *COMMON, *partial])
        for suite in ('images', 'compose'):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, 'is required for the registry suite'):
                gate.arguments(['--suite', suite, *COMMON, *REGISTRY])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            gate.arguments(['--suite', 'registry', *COMMON, *REGISTRY, '--registry-layout=/other'])

    def test_registry_rejects_builder_fixture_terminal_options_and_all_carries_them(self):
        for option in FOREIGN:
            with self.subTest(option=option), self.assertRaises(ValueError):
                gate.arguments(['--suite', 'registry', *COMMON, *REGISTRY, '--' + option, '/foreign/input'])
        # A composed run carries every suite's inputs, so the registry ones are
        # required rather than rejected, and the other suites' remain required too.
        with self.assertRaisesRegex(ValueError, 'is required for the registry suite'):
            gate.arguments(['--suite', 'all', *COMMON])
        with self.assertRaisesRegex(ValueError, '--tmux is required'):
            gate.arguments(['--suite', 'all', *COMMON, *REGISTRY])

    def test_registry_rejection_precedes_provisioning_inputs(self):
        # Suite/option admission fails before required startup options are demanded.
        with self.assertRaisesRegex(ValueError, 'is required for the registry suite'):
            gate.arguments(['--suite', 'registry'])
        with self.assertRaisesRegex(ValueError, 'required option'):
            gate.arguments(['--suite', 'registry', *REGISTRY])


class PreflightTests(unittest.TestCase):
    def test_registry_preflight_admits_inputs_before_startup_and_pins_closure(self):
        args = types.SimpleNamespace(suite='registry', fixture='/owned/base', image_input='/owned/pin',
            run_id='registry-owned', registry_archive='/owned/registry.tar', registry_layout='/owned/layout')
        order = []
        record = registry_record()
        def admit(archive, layout):
            order.append('admit')
            self.assertEqual((archive, layout), (Path('/owned/registry.tar'), Path('/owned/layout')))
            return copy.deepcopy(record)
        def startup_preflight(args, require_host):
            order.append('startup')
            return {'inputs': {}}
        def digest(path):
            return record['archive_sha256'] if str(path) == '/owned/registry.tar' else 'hash:' + str(path)
        with patch.object(subject, 'admit_inputs', side_effect=admit), \
                patch.object(gate.startup, 'preflight', side_effect=startup_preflight), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', side_effect=digest), \
                patch.object(gate.image_input, 'load', return_value={'immutable': 'image-pin'}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', return_value='fixture-hash'), \
                patch.object(gate, 'tmux_input') as terminal, \
                patch('linux_docker_buildkit_builder.preflight_archive') as builder:
            info = gate.preflight(args, require_host=False)
        self.assertEqual(order, ['admit', 'startup'])
        self.assertEqual(info['scope'], gate.REGISTRY_SCOPE)
        self.assertEqual(info['suite'], 'registry')
        self.assertEqual(info['registry'], record)
        self.assertEqual(info['registry_archive'], '/owned/registry.tar')
        self.assertEqual(info['registry_layout'], '/owned/layout')
        self.assertEqual(info['inputs']['/owned/registry.tar'], record['archive_sha256'])
        for name in subject.required_source_paths():
            self.assertEqual(info['inputs'][name], 'hash:' + name)
        closure = set(subject.required_source_paths())
        for name in ('linux_docker_registry_session.py', 'linux_docker_registry_secrets.py',
                     'linux_docker_registry_commands.py', 'linux_docker_private_stdin.py',
                     'linux_docker_registry_machine.py', 'registry-requirements.txt'):
            self.assertIn(str(subject.HELPERS / name), closure)
        self.assertIn(str(subject.PIN), closure)
        self.assertIn(str(subject.WRAPPER), closure)
        self.assertNotIn('buildkit', info)
        terminal.assert_not_called()
        builder.assert_not_called()

    def test_registry_admission_failure_prevents_startup_preflight(self):
        args = types.SimpleNamespace(suite='registry', fixture='/owned/base', image_input='/owned/pin',
            run_id='registry-owned', registry_archive='/owned/registry.tar', registry_layout='/owned/layout')
        with patch.object(subject, 'admit_inputs', side_effect=ValueError('registry archive: truncated')), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'preflight') as startup_preflight:
            with self.assertRaisesRegex(ValueError, 'truncated'):
                gate.preflight(args, require_host=False)
        startup_preflight.assert_not_called()

    def test_registry_preflight_rejects_foreign_or_missing_options_without_parser(self):
        base = dict(suite='registry', fixture='/owned/base', image_input='/owned/pin', run_id='registry-owned',
                    registry_archive='/owned/registry.tar', registry_layout='/owned/layout')
        with patch.object(subject, 'admit_inputs') as admit, patch.object(gate.startup, 'preflight') as startup_preflight:
            for option in FOREIGN:
                with self.subTest(option=option), self.assertRaises(ValueError):
                    gate.preflight(types.SimpleNamespace(**base, **{option.replace('-', '_'): '/foreign'}), require_host=False)
            for missing in ('registry_archive', 'registry_layout'):
                with self.subTest(missing=missing), self.assertRaises(ValueError):
                    gate.preflight(types.SimpleNamespace(**dict(base, **{missing: None})), require_host=False)
            with self.assertRaises(ValueError):
                gate.preflight(types.SimpleNamespace(**dict(base, suite='images')), require_host=False)
        admit.assert_not_called()
        startup_preflight.assert_not_called()

    def test_images_closure_unchanged_and_never_imports_registry_admission(self):
        args = types.SimpleNamespace(suite='images', fixture='/owned/base', image_input='/owned/pin', run_id='images-owned')
        with patch.object(subject, 'admit_inputs') as admit, \
                patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', side_effect=lambda path: 'hash:' + str(path)), \
                patch.object(gate.image_input, 'load', return_value={}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', return_value='fixture-hash'):
            info = gate.preflight(args, require_host=False)
        admit.assert_not_called()
        self.assertNotIn('registry', info)
        self.assertNotIn(str(subject.REQUIREMENTS), info['inputs'])

    def test_lock_must_pin_admitted_dependency_versions(self):
        self.assertEqual(subject.locked_dependencies({'cryptography': '50.0.1', 'bcrypt': '5.0.0'}),
                         {'cryptography': '50.0.1', 'bcrypt': '5.0.0'})
        for versions in ({'cryptography': '50.0.1', 'bcrypt': '4.9.9'}, {'unknown': '1.0'}, {}):
            with self.subTest(versions=versions), self.assertRaises(ValueError):
                subject.locked_dependencies(versions)

    def test_required_helpers_fail_closed_when_a_helper_disappears(self):
        with patch.object(subject.HELPERS.__class__, 'glob', return_value=iter([])):
            with self.assertRaisesRegex(ValueError, 'closure incomplete'):
                subject.required_source_paths()


class FakeControls:
    def __init__(self, events, fail=None):
        self.events, self.fail = events, fail

    def _step(self, name, *args):
        self.events.append(name)
        if name == self.fail:
            raise ValueError('mock ' + name + ' failure')
        return {'control': name, 'args': [copy.deepcopy(a) if isinstance(a, dict) else str(a) for a in args]}

    def baseline(self):
        return self._step('baseline')

    def check_after_login(self, descriptor):
        return self._step('check_after_login', descriptor)

    def check_after_logout(self, descriptor):
        return self._step('check_after_logout', descriptor)

    def final(self):
        return self._step('final')

    def replay_and_scan(self, session, output):
        return self._step('replay_and_scan', output)


class MachineTests(unittest.TestCase):
    @contextlib.contextmanager
    def case(self, *, fail=None, control_fail=None):
        with tempfile.TemporaryDirectory(prefix='vz-registry-machine-unit-') as temporary:
            root = Path(temporary).resolve()
            archive = root / 'registry.tar'
            archive.write_bytes(b'not a real archive; only its digest is pinned here')
            project = root / 'project'
            project.mkdir()
            events = []
            scope = {'project_id': 'project-1', 'environment_id': 'environment-1', 'machine_id': 'machine-1',
                     'machine_incarnation': 'incarnation-1', 'runtime_identity': 'runtime-1',
                     'docker_context': 'owned-machine', 'docker_endpoint': 'unix:///private/owned/docker.sock',
                     'engine_id': 'engine-1'}
            descriptor = {'owner': {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
                          'name': scope['docker_context'], 'endpoint': scope['docker_endpoint'],
                          'engine_id': scope['engine_id'], 'incarnation_id': scope['machine_incarnation'],
                          'config_dir': str(root / 'machine-config')}
            proof = {'receipt_path': '/original/runtime-proof.json', 'receipt_sha256': 'a' * 64}
            pin = {'reference': 'python@sha256:' + 'b' * 64, 'id': 'sha256:' + 'c' * 64, 'platform': 'linux/arm64'}
            images = {'base': dict(pin), 'compose': dict(pin)}
            record = registry_record(gate.startup.digest(archive))
            inputs = {name: driver.sha256(driver.regular(Path(name), subject.LIMIT)) for name in subject.required_source_paths()}
            inputs[str(archive)] = record['archive_sha256']
            harness = types.SimpleNamespace(evidence=root, descriptors=[copy.deepcopy(descriptor)],
                effects_uncertain=False, record=types.SimpleNamespace(receipts=[], canaries=[]),
                info={'run_id': 'registry-unit', 'python_image': dict(pin, extra='ignored'), 'inputs': inputs,
                      'registry': record, 'registry_archive': str(archive)},
                monitor=types.SimpleNamespace(check=Mock(side_effect=lambda: events.append('monitor'))),
                registry_sessions=[], registry_controls=FakeControls(events, control_fail),
                registry_project=str(project), sensitive_canaries=[])
            holder = types.SimpleNamespace(session=None, private=None)
            case = self
            class Secrets:
                @classmethod
                def generate(cls, owner, run_id, *, now_unix_ns):
                    case.assertEqual((owner, run_id), (descriptor['owner'], 'registry-unit'))
                    events.append('secrets')
                    holder.private = cls()
                    return holder.private
            class Session:
                def __init__(self, session_harness, session_descriptor, session_project, index, private):
                    case.assertIs(session_harness, harness)
                    case.assertEqual(session_descriptor, descriptor)
                    case.assertEqual((session_project, index), (str(project), 0))
                    case.assertIs(private, holder.private)
                    self.output = gate.startup.private(harness.evidence / ('registry-machine-' + str(index)))
                    self.prepared = self.workload_complete = self.cleanup_complete = self.failed = False
                    self.commands = types.SimpleNamespace(assert_certain=Mock(side_effect=lambda: events.append('commands-certain')))
                    self.documents = []
                    harness.registry_sessions.append(self)
                    holder.session = self
                    events.append('construct')
                def _step(self, name):
                    events.append(name)
                    if name == fail:
                        self.failed = True
                        raise ValueError('mock ' + name + ' failure')
                def document(self, name, value):
                    json.dumps(value)
                    self.documents.append(name)
                    gate.startup.document(self.output / name, value)
                def prepare(self):
                    self._step('prepare'); self.prepared = True
                def authenticate(self):
                    self._step('authenticate'); return {'route': 'proof'}
                def roundtrip(self):
                    self._step('roundtrip'); self.workload_complete = True
                    return {'remote_content': 'proof', 'release_certified': False}
                def cleanup(self):
                    self._step('cleanup'); self.cleanup_complete = True
                    return {'owned_registry_removed': True}
                def certain(self):
                    events.append('certain')
            secrets_module = types.ModuleType('linux_docker_registry_secrets'); secrets_module.Secrets = Secrets
            session_module = types.ModuleType('linux_docker_registry_session'); session_module.Session = Session
            with patch.dict(sys.modules, {'linux_docker_registry_secrets': secrets_module,
                                          'linux_docker_registry_session': session_module}):
                yield types.SimpleNamespace(harness=harness, descriptor=descriptor, scope=scope, proof=proof,
                    images=images, events=events, holder=holder, root=root, archive=archive,
                    invoke=lambda index=0: subject.run_machine(harness, descriptor, scope, proof, images, index))

    def workload_events(self, events):
        return [event for event in events if event != 'monitor']

    def test_full_sequence_cleanup_only_after_replay_and_scan(self):
        with self.case() as case:
            result = case.invoke()
            self.assertEqual(self.workload_events(case.events), [
                'secrets', 'construct', 'prepare', 'authenticate', 'check_after_login', 'roundtrip',
                'replay_and_scan', 'cleanup', 'check_after_logout', 'commands-certain', 'certain'])
            self.assertEqual(result['scope'], subject.SCOPE)
            self.assertFalse(result['release_certified'])
            self.assertFalse(result['docker_parity_certified'])
            self.assertEqual(result['test_case_retries'], 0)
            self.assertEqual(result['machine_scope'], case.scope)
            self.assertEqual(result['credential_controls']['after_login']['args'], [case.descriptor])
            self.assertEqual(result['independent_validation']['args'], [str(case.root / 'registry-machine-0')])
            self.assertEqual(set(result['timings']), {'prepare', 'authenticate', 'controls_after_login', 'roundtrip',
                                                      'replay_and_scan', 'cleanup', 'controls_after_logout'})
            self.assertEqual(case.holder.session.documents, ['registry-machine.intent.json', 'workload.json',
                'independent-replay.json', 'machine-registry-validation.json'])
            retained = json.loads((case.root / 'registry-machine-0/machine-registry-validation.json').read_bytes())
            self.assertEqual(retained, json.loads(json.dumps(result)))
            self.assertEqual(case.harness.registry_sessions, [case.holder.session])

    def test_session_failure_never_calls_cleanup_and_keeps_session_registered(self):
        for stage in ('prepare', 'authenticate', 'roundtrip'):
            with self.subTest(stage=stage), self.case(fail=stage) as case:
                with self.assertRaisesRegex(ValueError, 'mock ' + stage + ' failure'):
                    case.invoke()
                self.assertNotIn('cleanup', case.events)
                self.assertNotIn('check_after_logout', case.events)
                self.assertEqual(case.harness.registry_sessions, [case.holder.session])
                self.assertTrue(case.holder.session.failed)
                self.assertFalse(case.holder.session.cleanup_complete)
                self.assertFalse((case.root / 'registry-machine-0/machine-registry-validation.json').exists())

    def test_failed_controls_or_replay_withhold_cleanup(self):
        for stage in ('check_after_login', 'replay_and_scan'):
            with self.subTest(stage=stage), self.case(control_fail=stage) as case:
                with self.assertRaisesRegex(ValueError, 'mock ' + stage + ' failure'):
                    case.invoke()
                self.assertNotIn('cleanup', case.events)
                self.assertFalse(case.holder.session.cleanup_complete)
                self.assertEqual(case.harness.registry_sessions, [case.holder.session])
        with self.case(control_fail='check_after_logout') as case:
            with self.assertRaisesRegex(ValueError, 'mock check_after_logout failure'):
                case.invoke()
            self.assertIn('cleanup', case.events)
            self.assertFalse((case.root / 'registry-machine-0/machine-registry-validation.json').exists())

    def test_failed_cleanup_leaves_no_validation_and_no_logout_control(self):
        with self.case(fail='cleanup') as case:
            with self.assertRaisesRegex(ValueError, 'mock cleanup failure'):
                case.invoke()
            self.assertNotIn('check_after_logout', case.events)
            self.assertFalse(case.holder.session.cleanup_complete)

    def test_foreign_descriptor_scope_or_missing_proof_rejected_before_secrets(self):
        for field in ('machine_id', 'machine_incarnation', 'docker_context', 'docker_endpoint', 'engine_id'):
            with self.subTest(field=field), self.case() as case:
                case.scope[field] = 'foreign'
                with self.assertRaises(ValueError): case.invoke()
                self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.descriptors = []
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.proof.clear()
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.images['base'] = {'reference': 'foreign', 'id': 'sha256:' + '9' * 64, 'platform': 'linux/arm64'}
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])

    def test_missing_controls_project_or_changed_pins_prevent_dispatch(self):
        with self.case() as case:
            case.harness.registry_controls = None
            with self.assertRaisesRegex(ValueError, 'controls required'): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.registry_project = None
            with self.assertRaisesRegex(ValueError, 'project directory'): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            path = subject.required_source_paths()[0]
            case.harness.info['inputs'][path] = '0' * 64
            with self.assertRaisesRegex(ValueError, 'source changed'): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            del case.harness.info['inputs'][str(subject.WRAPPER)]
            with self.assertRaises((ValueError, KeyError)): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.archive.write_bytes(b'changed archive bytes')
            with self.assertRaisesRegex(ValueError, 'archive changed'): case.invoke()
            self.assertEqual(case.events, [])
        for key, value in (('manifest_digest', 'sha256:' + '1' * 64), ('binary_sha256', '2' * 64),
                           ('go_version', 'go1.24.0'), ('version', 'v3.0.0'), ('release_certified', True)):
            with self.subTest(key=key), self.case() as case:
                case.harness.info['registry'][key] = value
                with self.assertRaisesRegex(ValueError, 'admission record differs'): case.invoke()
                self.assertEqual(case.events, [])

    def test_existing_output_incomplete_earlier_session_or_bad_index_rejected(self):
        with self.case() as case:
            (case.root / 'registry-machine-0').mkdir()
            with self.assertRaisesRegex(ValueError, 'preexists'): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.registry_sessions.append(types.SimpleNamespace(cleanup_complete=False))
            with self.assertRaisesRegex(ValueError, 'earlier registry Session'): case.invoke(1)
            self.assertEqual(case.events, [])
        with self.case() as case:
            with self.assertRaises(ValueError): case.invoke(True)
            with self.assertRaises(ValueError): case.invoke(4)
            self.assertEqual(case.events, [])


class ScenarioTests(unittest.TestCase):
    def scenario(self, fail_at=None):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        image_pin = {'reference': 'python@sha256:' + 'b' * 64, 'id': 'sha256:' + 'c' * 64, 'platform': 'linux/arm64',
                     'extra_registry_metadata': 'not passed as image identity'}
        harness.info = {'suite': 'registry', 'public_ca': {'bundle_sha256': 'a' * 64}, 'python_image': image_pin}
        harness.drivers, harness.driver_cleanup_verified = [], []
        harness.record = types.SimpleNamespace(receipts=[], pending_interactions=[])
        harness.effects_uncertain = False
        harness.registry_sessions, harness.registry_controls, harness.registry_project = [], None, None
        harness.runtime_audits, harness.keep_proofs_verified, harness.builders, harness.owned = [], [], [], []
        harness.ssh_cache_requests, harness.ssh_cache_proofs = [], []
        harness.cli, harness.evidence = Path('/owned/bin/vz'), Path('/owned/evidence')
        contexts = [{'name': 'context-' + str(i), 'endpoint': 'endpoint-' + str(i), 'engine_id': 'engine-' + str(i),
                     'config_dir': '/owned/machine-config-' + str(i)} for i in range(4)]
        environments = [{'environment_id': 'env-' + str(i), 'machines': [
            {'machine_id': 'machine-' + str(2 * i + j), 'name': 'worker-' + str(j),
             'docker_context': contexts[2 * i + j]} for j in range(2)]} for i in range(2)]
        project = Path('/owned/project')
        harness.project, harness.up = Mock(return_value=project), Mock(side_effect=environments)
        harness.daemon_fingerprint = Mock(return_value='daemon')
        harness.inspect = Mock(side_effect=[contexts[:2], contexts[2:], contexts[:2], contexts[2:]])
        harness.status = Mock()
        harness.command = Mock(return_value=(('a' * 64 + '  /etc/vz/ca-certificates.crt\n').encode(), b'', 0))
        events = []
        harness.sentinel = Mock(side_effect=lambda descriptor: events.append('sentinel') or {'descriptor': descriptor})
        harness.enroll_runtime_audits = Mock()
        harness.prepare_image, harness.prepare_builder = Mock(), Mock()
        harness.docker, harness.mutate = Mock(), Mock()
        harness.driver_inputs, harness.validate_driver = Mock(), Mock()
        monitor = Mock()
        monitor.record = types.SimpleNamespace(receipts=[], pending_interactions=[])
        monitor.thread.is_alive.return_value = False
        monitor.summary.return_value = {'samples': 'observed'}
        monitor.start.side_effect = lambda: events.append('monitor-start')
        constructed = {}
        class Controls(FakeControls):
            def __init__(self, controls_harness, all_contexts, selected, sentinel):
                super().__init__(events)
                constructed['args'] = (controls_harness, all_contexts, selected, sentinel)
                events.append('controls')
        def run_machine(machine_harness, descriptor, scope, proof, images, index):
            events.append('machine-' + str(index))
            self.assertIs(machine_harness.registry_controls, constructed['controls'])
            session = types.SimpleNamespace(cleanup_complete=index != fail_at, failed=index == fail_at,
                                            commands=types.SimpleNamespace(assert_certain=Mock()), certain=Mock())
            machine_harness.registry_sessions.append(session)
            if index == fail_at:
                raise ValueError('registry Machine replay failed')
            return {'operation': index}
        controls_module = types.ModuleType('linux_docker_registry_controls')
        def make_controls(*args):
            constructed['controls'] = Controls(*args)
            return constructed['controls']
        controls_module.Controls = make_controls
        machine_module = types.SimpleNamespace(run_machine=Mock(side_effect=run_machine))
        with patch.dict(sys.modules, {'linux_docker_registry_controls': controls_module,
                                      'linux_docker_registry_machine': machine_module}), \
                patch.object(gate, 'SentinelMonitor', return_value=monitor), \
                patch.object(gate.startup, 'exact_developer_topology'), \
                patch.object(gate.startup, 'document'), \
                patch.object(gate, 'authenticated_proof', return_value=({'scope': 'exact'}, {'proof': 'exact'})), \
                patch.object(gate.driver, 'Driver') as selected:
            if fail_at is None:
                result = harness.scenario()
            else:
                result = None
                with self.assertRaisesRegex(ValueError, 'registry Machine replay failed'):
                    harness.scenario()
        selected.assert_not_called()
        harness.prepare_image.assert_not_called()
        harness.prepare_builder.assert_not_called()
        harness.enroll_runtime_audits.assert_not_called()
        return types.SimpleNamespace(harness=harness, result=result, events=events, constructed=constructed,
                                     contexts=contexts, monitor=monitor, machine=machine_module, project=project)

    def test_controls_bracket_three_machines_and_fourth_is_sentinel(self):
        case = self.scenario()
        self.assertEqual(case.events, ['controls', 'sentinel', 'sentinel', 'sentinel', 'sentinel', 'monitor-start',
                                       'baseline', 'machine-0', 'machine-1', 'machine-2', 'final'])
        harness, contexts, selected, sentinel = case.constructed['args']
        self.assertIs(harness, case.harness)
        self.assertEqual(contexts, case.contexts)
        self.assertEqual(selected, case.contexts[:3])
        self.assertEqual(sentinel, case.contexts[3])
        self.assertEqual(case.harness.registry_project, str(case.project))
        self.assertEqual(case.result['machine_slices'], [{'operation': i} for i in range(3)])
        self.assertEqual(case.result['credential_controls'], {'baseline': {'control': 'baseline', 'args': []},
                                                              'final': {'control': 'final', 'args': []}})
        base = {key: case.harness.info['python_image'][key] for key in ('reference', 'id', 'platform')}
        self.assertEqual(case.machine.run_machine.call_args_list, [unittest.mock.call(
            case.harness, case.contexts[i], {'scope': 'exact'}, {'proof': 'exact'}, {'base': base, 'compose': base}, i)
            for i in range(3)])
        case.monitor.stop.assert_called_once_with()
        self.assertEqual(case.harness.status.call_count, 2)

    def test_failed_machine_skips_final_controls_later_machines_and_fences_cleanup(self):
        for fail_at in (0, 1, 2):
            with self.subTest(fail_at=fail_at):
                case = self.scenario(fail_at=fail_at)
                self.assertNotIn('final', case.events)
                self.assertEqual([e for e in case.events if e.startswith('machine-')], ['machine-' + str(i) for i in range(fail_at + 1)])
                case.monitor.stop.assert_called_once_with()
                case.harness.status.assert_not_called()
                with self.assertRaisesRegex(ValueError, 'registry Session lacks completed cleanup'):
                    case.harness.remove_owned()
                case.harness.docker.assert_not_called()
                case.harness.mutate.assert_not_called()


class FinalizationTests(unittest.TestCase):
    def harness(self, sessions):
        h = gate.ComposeHarness.__new__(gate.ComposeHarness)
        h.runtime_audits, h.keep_proofs_verified, h.builders, h.owned = [], [], [], []
        h.ssh_cache_requests, h.ssh_cache_proofs = [], []
        h.record = types.SimpleNamespace(receipts=[], pending_interactions=[])
        h.drivers, h.driver_cleanup_verified, h.monitor, h.effects_uncertain = [], [], None, False
        h.registry_sessions = sessions
        h.docker, h.mutate, h.exact_absent = Mock(), Mock(), Mock()
        return h

    def session(self, *, cleanup_complete=True, failed=False, commands_error=None, certain_error=None):
        return types.SimpleNamespace(cleanup_complete=cleanup_complete, failed=failed,
            commands=types.SimpleNamespace(assert_certain=Mock(side_effect=commands_error)),
            certain=Mock(side_effect=certain_error))

    def test_complete_certain_sessions_admit_owned_removal(self):
        sessions = [self.session() for _ in range(3)]
        h = self.harness(sessions)
        h.owned = [{'descriptor': {'name': 'exact'}, 'token': 'owned', 'tag': 'owned:fixture', 'image_id': 'sha256:' + 'a' * 64}]
        h.docker.return_value = (json.dumps([{'Id': 'sha256:' + 'a' * 64, 'Config': {'Labels': {gate.LABEL: 'owned'}}}]).encode(), b'', 0)
        h.remove_owned()
        for session in sessions:
            session.commands.assert_certain.assert_called()
            session.certain.assert_called()
        h.mutate.assert_called_once()

    def test_incomplete_failed_or_uncertain_session_withholds_all_removal(self):
        cases = {'incomplete': self.session(cleanup_complete=False),
                 'failed': self.session(failed=True),
                 'uncertain-commands': self.session(commands_error=ValueError('private registry commands remain uncertain')),
                 'uncertain-session': self.session(certain_error=ValueError('registry session requires reconciliation'))}
        for name, bad in cases.items():
            with self.subTest(name=name):
                h = self.harness([self.session(), bad])
                h.owned = [{'descriptor': {'name': 'exact'}, 'token': 'owned', 'tag': 'owned:fixture', 'image_id': 'sha256:' + 'a' * 64}]
                with self.assertRaises(ValueError):
                    h.remove_owned()
                h.docker.assert_not_called()
                h.mutate.assert_not_called()
                h.exact_absent.assert_not_called()

    def test_run_requires_three_completed_sessions_and_unchanged_archive(self):
        info = {'scope': gate.REGISTRY_SCOPE, 'suite': 'registry', 'inputs': {}, 'fixture': '/owned/base',
                'fixture_sha256': 'base-hash', 'registry_archive': '/owned/registry.tar',
                'registry': {'archive_sha256': 'archive-hash'}}
        for problem in (None, 'two-sessions', 'incomplete', 'archive'):
            with self.subTest(problem=problem):
                sessions = [self.session() for _ in range(2 if problem == 'two-sessions' else 3)]
                if problem == 'incomplete':
                    sessions[1].cleanup_complete = False
                harness = types.SimpleNamespace(evidence=Path('/owned/evidence'), root=Path('/owned/root'),
                    staged_inputs={}, monitor=None, stage=Mock(), scenario=Mock(return_value={}),
                    remove_owned=Mock(), cleanup=Mock(return_value={}), registry_sessions=sessions)
                digest = lambda path: 'changed' if problem == 'archive' and str(path) == '/owned/registry.tar' else 'archive-hash'
                with patch.object(gate, 'ComposeHarness', return_value=harness), \
                        patch.object(gate.os, 'umask'), patch.object(gate.startup, 'document'), \
                        patch.object(gate.startup, 'collect_runtime_receipts'), \
                        patch.object(gate.startup, 'checksum_evidence'), \
                        patch.object(gate.startup, 'digest', side_effect=digest), \
                        patch.object(gate.driver, 'tree_digest', return_value='base-hash'), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    code = gate.run(info)
                result = json.loads(output.getvalue())
                self.assertEqual(code, int(problem is not None))
                self.assertEqual(result['scope'], gate.REGISTRY_SCOPE)
                self.assertFalse(result['docker_parity_certified'])
                self.assertFalse(result['aggregate_release_certified'])
                self.assertEqual(result['release_scenarios_passed'], [])
                if problem is None:
                    self.assertEqual(result['outcome'], 'passed_dev_installed_registry_slice')
                elif problem == 'archive':
                    self.assertIn('archive changed', result['error'])
                else:
                    self.assertIn('three completed registry Sessions', result['error'])


class WrapperTests(unittest.TestCase):
    def test_wrapper_fixes_suite_and_uses_isolated_dependencies(self):
        raw = subject.WRAPPER.read_text()
        self.assertTrue(os.access(subject.WRAPPER, os.X_OK))
        self.assertIn('set -euo pipefail', raw)
        self.assertIn('uv run --no-project --python /usr/bin/python3', raw)
        self.assertIn('--with-requirements "$script_dir/helpers/registry-requirements.txt"', raw)
        self.assertIn('--suite registry "$@"', raw)
        self.assertIn('--suite|--suite=*)', raw)
        self.assertNotIn('pip install', raw)


if __name__ == '__main__':
    unittest.main()
