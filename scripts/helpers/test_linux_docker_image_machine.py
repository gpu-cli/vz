"""Offline Machine-adapter ownership tests; no Docker/VM/build dispatch."""
import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_image_machine as subject

FIXTURE = Path(__file__).resolve().parents[2] / 'tests/fixtures/vz-0.4/docker'


class Machine(unittest.TestCase):
    @contextmanager
    def case(self, *, fail=None, change=None, admission_none=False):
        with tempfile.TemporaryDirectory(prefix='vz-image-machine-unit-') as temporary:
            root = Path(temporary).resolve()
            events = []
            scope = {'project_id': 'project-1', 'environment_id': 'environment-1', 'machine_id': 'machine-1',
                'machine_incarnation': 'incarnation-1', 'runtime_identity': 'runtime-1',
                'docker_context': 'owned-machine', 'docker_endpoint': 'unix:///private/owned/docker.sock',
                'engine_id': 'engine-1'}
            descriptor = {'owner': {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
                'name': scope['docker_context'], 'endpoint': scope['docker_endpoint'],
                'engine_id': scope['engine_id'], 'incarnation_id': scope['machine_incarnation']}
            proof = {'receipt_path': '/original/runtime-proof.json', 'receipt_sha256': 'a' * 64}
            pin = {'reference': 'python@sha256:' + 'b' * 64, 'id': 'sha256:' + 'c' * 64, 'platform': 'linux/arm64'}
            images = {'base': copy.deepcopy(pin), 'compose': copy.deepcopy(pin)}
            record = lambda: SimpleNamespace(count=0, receipts=[], pending_interactions=[])
            existing = SimpleNamespace(record=record())
            harness = SimpleNamespace(evidence=root, descriptors=[copy.deepcopy(descriptor)],
                effects_uncertain=False, record=record(), drivers=[existing], driver_cleanup_verified=[True],
                info={'fixture': str(FIXTURE), 'inputs': {name: subject.driver.sha256(
                    subject.driver.regular(Path(name), subject.LIMIT)) for name in subject.required_source_paths()}},
                monitor=SimpleNamespace(check=Mock(side_effect=lambda: events.append('monitor'))))
            mapping = {'scope': copy.deepcopy(scope), 'runtime_evidence': copy.deepcopy(proof),
                'images': copy.deepcopy(images), 'fixture_sha256': subject.driver.tree_digest(FIXTURE)}
            holder = SimpleNamespace(selected=None, lane=None)
            def step(name):
                events.append(name)
                if name == fail:
                    raise ValueError('mock ' + name + ' failure')
                if change is not None:
                    change(name, harness, holder)
            def admitted(inputs, *, suite):
                step('admission')
                self.assertEqual(suite, 'compose')
                return SimpleNamespace(raw=inputs, verify_runtime_evidence=Mock(
                    return_value=None if admission_none else {'authenticated': True}))
            def new_driver(inputs, fixture, output):
                step('construct-driver')
                self.assertEqual(harness.driver_cleanup_verified, [True])
                self.assertEqual(fixture, FIXTURE)
                output.mkdir(mode=0o700)
                selected = SimpleNamespace(inputs=inputs, fixture=fixture, output=output,
                    env={'LC_ALL': 'C'}, record=record())
                holder.selected = selected
                return selected
            def result(cleanup):
                return {'schema_version': 1, 'command_count': 57 if cleanup else 42,
                    'workload_complete': True, 'cleanup_complete': cleanup,
                    'full_baseline_restored': cleanup, 'subject_absent': True,
                    'decoy_retained': not cleanup, 'physical_execution_certified': False,
                    'references': {'source': 'source-selected'}, 'baseline': {'foreign': {'kept': True}}}
            class Lane:
                def __init__(self, selected):
                    self.selected = selected
                    holder.lane = self
                    self_case.assertIs(harness.drivers[-1], selected)
                    self_case.assertEqual(harness.driver_cleanup_verified, [True, False])
                    self.cleaned = False
                    step('construct-lane')
                def exercise(self):
                    self_case.assertTrue((self.selected.output / 'inputs.json').exists())
                    self_case.assertTrue((self.selected.output / 'image-machine.intent.json').exists())
                    step('exercise')
                    self.selected.record.count = 42
                    self.selected.record.receipts.append({'effects_uncertain': False})
                    return result(False)
                def cleanup(self):
                    step('cleanup')
                    self.selected.record.count = 57
                    self.cleaned = True
                    return result(True)
            self_case = self
            def replay(output, inputs, *, environment, cleanup=False):
                self.assertEqual(output, holder.selected.output)
                self.assertEqual(inputs, mapping)
                self.assertEqual(environment, holder.selected.env)
                self.assertIs(harness.driver_cleanup_verified[1], False)
                step('replay-cleanup' if cleanup else 'replay-workload')
                return result(cleanup)
            module = ModuleType('linux_docker_e2e')
            module.input_mapping = Mock(side_effect=lambda *args: copy.deepcopy(mapping))
            with ExitStack() as stack:
                stack.enter_context(patch.dict(sys.modules, {'linux_docker_e2e': module}))
                stack.enter_context(patch.object(subject.driver, 'Inputs', side_effect=admitted))
                stack.enter_context(patch.object(subject.driver, 'Driver', side_effect=new_driver))
                stack.enter_context(patch.object(subject.image, 'ImageRoundTrip', Lane))
                stack.enter_context(patch.object(subject.image, 'replay', side_effect=replay))
                yield SimpleNamespace(harness=harness, scope=scope, proof=proof, images=images,
                    descriptor=descriptor, events=events, holder=holder, mapping=mapping,
                    invoke=lambda: subject.run_machine(harness, descriptor, scope, proof, images, 0))

    def test_registered_before_workload_final_replay_then_only_owned_flag_admitted(self):
        with self.case() as case:
            result = case.invoke()
            self.assertEqual(case.harness.driver_cleanup_verified, [True, True])
            self.assertEqual([event for event in case.events if event != 'monitor'],
                ['admission', 'construct-driver', 'construct-lane', 'exercise', 'replay-workload', 'cleanup', 'replay-cleanup'])
            self.assertIs(case.harness.drivers[1].image_roundtrip, case.holder.lane)
            self.assertEqual(result['machine_scope'], case.scope)
            self.assertEqual(result['scope'], subject.SCOPE)
            self.assertFalse(result['image_build_dispatched_by_adapter'])
            self.assertFalse(result['docker_parity_certified'])
            self.assertFalse(result['release_acceptance_certified'])
            self.assertEqual(result['test_case_retries'], 0)
            self.assertEqual(subject.retained(case.holder.selected.output, 'machine-image-validation.json'), result)

    def test_each_failure_preserves_registered_driver_and_never_auto_cleans(self):
        for stage in ('construct-lane', 'exercise', 'replay-workload', 'cleanup', 'replay-cleanup'):
            with self.subTest(stage=stage), self.case(fail=stage) as case:
                with self.assertRaisesRegex(ValueError, 'mock ' + stage + ' failure'):
                    case.invoke()
                self.assertEqual(case.harness.driver_cleanup_verified, [True, False])
                self.assertIs(case.harness.drivers[1], case.holder.selected)
                if stage in ('construct-lane', 'exercise', 'replay-workload'):
                    self.assertNotIn('cleanup', case.events)
                self.assertFalse((case.holder.selected.output / 'machine-image-validation.json').exists())

    def test_foreign_scope_incarnation_endpoint_or_descriptor_rejected_before_driver(self):
        for field in ('machine_id', 'machine_incarnation', 'docker_context', 'docker_endpoint', 'engine_id'):
            with self.subTest(field=field), self.case() as case:
                case.scope[field] = 'foreign'
                with self.assertRaises(ValueError): case.invoke()
                self.assertEqual(case.events, [])
        with self.case() as case:
            case.harness.descriptors = []
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])

    def test_missing_or_unverified_runtime_proof_rejected_without_driver(self):
        with self.case() as case:
            case.proof.clear()
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])
        with self.case(admission_none=True) as case:
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, ['admission'])

    def test_input_mapping_cannot_replace_external_authentication(self):
        for field in ('scope', 'runtime_evidence', 'images'):
            with self.subTest(field=field), self.case() as case:
                case.mapping[field] = {'self_consistent_but_foreign': True}
                with self.assertRaises(ValueError): case.invoke()
                self.assertEqual(case.events, [])

    def test_missing_or_changed_source_pin_prevents_driver_and_dispatch(self):
        for missing in (False, True):
            with self.subTest(missing=missing), self.case() as case:
                path = subject.required_source_paths()[0]
                if missing: del case.harness.info['inputs'][path]
                else: case.harness.info['inputs'][path] = '0' * 64
                with self.assertRaises((ValueError, KeyError)): case.invoke()
                self.assertEqual(case.events, [])

    def test_uncertain_or_pending_earlier_recorder_withholds_first_mutation(self):
        for change in ('harness', 'recorder', 'pending', 'monitor'):
            with self.subTest(change=change), self.case() as case:
                if change == 'harness': case.harness.effects_uncertain = True
                elif change == 'recorder': case.harness.drivers[0].record.receipts = [{'effects_uncertain': True}]
                elif change == 'pending': case.harness.record.pending_interactions = [object()]
                else: case.harness.monitor.check.side_effect = ValueError('monitor failed')
                with self.assertRaises(ValueError): case.invoke()
                self.assertNotIn('exercise', case.events)
                self.assertNotIn('cleanup', case.events)
                self.assertEqual(case.harness.driver_cleanup_verified, [True, False])

    def test_changed_live_inputs_do_not_mutate_original_expectation(self):
        def change(stage, harness, holder):
            if stage == 'exercise': holder.selected.inputs.raw['scope']['engine_id'] = 'foreign'
        with self.case(change=change) as case:
            with self.assertRaisesRegex(ValueError, 'inputs changed'): case.invoke()
            self.assertEqual(case.scope['engine_id'], 'engine-1')
            self.assertNotIn('cleanup', case.events)
            self.assertEqual(case.harness.driver_cleanup_verified, [True, False])

    def test_resealed_retained_documents_do_not_manufacture_success(self):
        original = subject.startup.document
        for name in ('inputs.json', 'image-machine.intent.json', 'workload.json', 'cleanup.json', 'machine-image-validation.json'):
            def tamper(path, value):
                if path.name == name: value = dict(value, forged=True)
                original(path, value)
            with self.subTest(name=name), self.case() as case, \
                    patch.object(subject.startup, 'document', side_effect=tamper):
                with self.assertRaises(ValueError): case.invoke()
                self.assertEqual(case.harness.driver_cleanup_verified, [True, False])
                if name in ('inputs.json', 'image-machine.intent.json', 'workload.json'):
                    self.assertNotIn('cleanup', case.events)

    def test_late_unknown_commands_and_uncertainty_withhold_cleanup_flag(self):
        for mutation in ('count', 'uncertain', 'runtime-audit', 'previous-semantic-failure'):
            def change(stage, harness, holder):
                if stage == 'replay-cleanup':
                    if mutation == 'count': holder.selected.record.count += 1
                    elif mutation == 'uncertain': holder.selected.record.receipts.append({'effects_uncertain': True})
                    elif mutation == 'previous-semantic-failure': harness.driver_cleanup_verified[0] = False
                    else:
                        harness.runtime_audits = [SimpleNamespace(assert_enrolled_certain=Mock(
                            side_effect=ValueError('uncertain runtime audit')))]
            with self.subTest(mutation=mutation), self.case(change=change) as case:
                with self.assertRaises(ValueError): case.invoke()
                self.assertEqual(case.harness.driver_cleanup_verified,
                    [mutation != 'previous-semantic-failure', False])

    def test_existing_evidence_and_boolean_index_do_not_retry(self):
        with self.case() as case:
            (case.harness.evidence / 'image-machine-0').mkdir()
            with self.assertRaises(ValueError): case.invoke()
            self.assertEqual(case.events, [])
        with self.case() as case:
            with self.assertRaises(ValueError):
                subject.run_machine(case.harness, case.descriptor, case.scope, case.proof, case.images, True)
            self.assertEqual(case.events, [])


if __name__ == '__main__':
    unittest.main()
