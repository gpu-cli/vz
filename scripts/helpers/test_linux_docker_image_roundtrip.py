"""Finite in-memory recipe/dispatch/replay adversaries; never invoke Docker."""
import copy
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import linux_docker_image_roundtrip as lane

INPUTS = {'run_id': 'image-test-123', 'docker_config': '/private/owned/docker',
          'scope': {'machine_id': 'machine-1', 'docker_context': 'exact-machine',
                    'engine_id': 'engine-1', 'docker_endpoint': 'unix:///private/owned/engine.sock'},
          'clients': {'docker': {'path': '/pinned/docker'}}}
FOREIGN = 'sha256:' + 'e' * 64


def image(role, refs):
    selected = lane.fixture.fixture(role)
    tags = sorted(lane.fixture.familiar_reference(ref) for ref in refs)
    return {'Id': selected['manifest_digest'], 'RepoTags': tags,
            'RepoDigests': [tags[0].rsplit(':', 1)[0] + '@' + selected['manifest_digest']],
            'Config': selected['config']['config'], 'RootFS': {'Type': 'layers', 'Layers': [selected['diff_id']]},
            'Architecture': 'arm64', 'Os': 'linux', 'Variant': None, 'Created': lane.fixture.CREATED}


class Fake:
    def __init__(self, *, fail=None):
        self.inputs = copy.deepcopy(INPUTS)
        self.count = 0
        self.commands = {}
        self.guards = []
        self.images = {FOREIGN: {'Id': FOREIGN, 'RepoTags': ['unrelated:keep'], 'RepoDigests': [],
            'Config': {'Labels': {'owner': 'unrelated'}}, 'RootFS': {'Type': 'layers', 'Layers': []},
            'Architecture': 'arm64', 'Os': 'linux', 'Variant': None, 'Created': lane.fixture.CREATED}}
        self.fail = fail
        self.load_count = self.save_count = self.remove_count = 0

    def guard(self):
        self.guards.append(self.count + 1)
        self.count += 2

    def command(self, args, *, plan=None):
        self.count += 1
        stdout = b''
        if args == ['image', 'ls', '--all', '--quiet', '--no-trunc']:
            stdout = ''.join(key + '\n' for key in sorted(self.images)).encode()
        elif args[:2] == ['image', 'inspect']:
            target = args[-1]
            if target.startswith('sha256:'):
                row = self.images[target]
            else:
                familiar = lane.fixture.familiar_reference(target)
                row = next(value for value in self.images.values() if familiar in value['RepoTags'])
            stdout = lane.canonical([row])
        elif args[:2] == ['image', 'load']:
            self.load_count += 1
            self.assert_plan(plan, loading=True)
            raw = plan['actions'][0]['data']
            # Fixture archive's independent parser selects reference/role here;
            # this simulates Engine loading, not source-program expectations.
            files, _ = lane.archive_verifier._tar(raw, outer=True)
            compatible = json.loads(files['manifest.json'])
            reference = lane.fixture.PREFIX + compatible[0]['RepoTags'][0]
            config = json.loads(files[compatible[0]['Config']])
            role = config['config']['Labels']['com.vz.fixture.role']
            row = image(role, [reference])
            self.images[row['Id']] = row
            if self.fail == 'wrong-image' and self.load_count == 1:
                row['Architecture'] = 'amd64'
            stdout = ('Loaded image: ' + lane.fixture.familiar_reference(reference) + '\n').encode()
        elif args[:2] == ['image', 'tag']:
            source, alias = [lane.fixture.familiar_reference(ref) for ref in args[-2:]]
            row = next(value for value in self.images.values() if source in value['RepoTags'])
            row['RepoTags'] = sorted(row['RepoTags'] + [alias])
            if self.fail == 'decoy-drift':
                self.images[lane.fixture.fixture('decoy')['manifest_digest']]['Config']['WorkingDir'] = '/foreign'
            if self.fail == 'extra-tag':
                row['RepoTags'].append('foreign:tag')
        elif args[:2] == ['image', 'save']:
            self.save_count += 1
            self.assert_plan(plan, loading=False)
            stdout = lane.fixture.archive('subject', args[-1])
            if self.save_count == 2:
                stdout += b'\0' * 1024  # Valid changed padding, stable semantic content.
            if self.fail == 'bad-save':
                stdout = stdout[:-1] + b'x'
        elif args[:2] == ['image', 'rm']:
            self.remove_count += 1
            reference = lane.fixture.familiar_reference(args[-1])
            key = next(key for key, value in self.images.items() if reference in value['RepoTags'])
            self.images[key]['RepoTags'].remove(reference)
            last = not self.images[key]['RepoTags']
            if last:
                del self.images[key]
            if self.fail == 'baseline-drift':
                self.images[FOREIGN]['Config']['Labels']['owner'] = 'changed'
            stdout = ('Untagged: ' + reference + '\n' + ('Deleted: ' + key + '\n' if last else '')).encode()
        else:
            raise AssertionError('unexpected source command: ' + repr(args))
        result = SimpleNamespace(index=self.count, stdout=stdout, stderr=b'',
                                 started_unix_ns=self.count * 10, finished_unix_ns=self.count * 10 + 1)
        self.commands[self.count] = (copy.deepcopy(args), copy.deepcopy(plan), result)
        return result

    @staticmethod
    def assert_plan(plan, loading):
        assert plan['mode'] == 'pipes' and plan['timeout_seconds'] == 30
        assert plan['actions'][-1] == {'kind': 'close_stdin'}
        assert [action['kind'] for action in plan['actions']] == (['write', 'close_stdin'] if loading else ['close_stdin'])


class RoundTripTests(unittest.TestCase):
    def test_complete_recipe_leaves_decoy_then_explicit_cleanup_restores_baseline(self):
        fake = Fake()
        baseline = copy.deepcopy(fake.images)
        program = lane._Program(fake)
        proof = program.exercise()
        self.assertTrue(proof['workload_complete'])
        self.assertFalse(proof['cleanup_complete'])
        self.assertTrue(proof['decoy_retained'])
        self.assertEqual(set(fake.images), {FOREIGN, lane.fixture.fixture('decoy')['manifest_digest']})
        self.assertNotEqual(proof['first_save']['archive_sha256'], proof['second_save']['archive_sha256'])
        for key in ('manifest_digest', 'config_digest', 'layer_digest', 'diff_id', 'payload_sha256'):
            self.assertEqual(proof['first_save'][key], proof['second_save'][key])
        final = program.cleanup()
        self.assertTrue(final['cleanup_complete'])
        self.assertTrue(final['full_baseline_restored'])
        self.assertFalse(final['physical_execution_certified'])
        self.assertEqual(fake.images, baseline)
        self.assertLess(fake.count, lane.MAX_COMMANDS)
        for args, _, _ in fake.commands.values():
            self.assertNotIn('--force', args)
            self.assertNotIn('prune', args)
            self.assertNotIn('pull', args)

    def test_exact_saved_bytes_are_reloaded_and_mutations_have_immediate_guards(self):
        fake = Fake()
        lane._Program(fake).exercise()
        saves = [result.stdout for args, _, result in fake.commands.values() if args[:2] == ['image', 'save']]
        loads = [plan['actions'][0]['data'] for args, plan, _ in fake.commands.values() if args[:2] == ['image', 'load']]
        self.assertEqual(len(loads), 3)
        self.assertEqual(loads[-1], saves[0])
        for index, (args, _, _) in fake.commands.items():
            if args[:2] in (['image', 'load'], ['image', 'tag'], ['image', 'rm']):
                self.assertIn(index - 2, fake.guards)
                self.assertIn(index + 1, fake.guards)

    def test_failures_do_not_run_decoy_cleanup_or_repair(self):
        for failure in ('wrong-image', 'decoy-drift', 'extra-tag', 'bad-save', 'baseline-drift'):
            fake = Fake(fail=failure)
            program = lane._Program(fake)
            with self.subTest(failure=failure), self.assertRaises(ValueError):
                program.exercise()
            count = fake.count
            with self.assertRaises(ValueError):
                program.cleanup()
            self.assertEqual(fake.count, count)
            self.assertFalse(any(args[:2] == ['image', 'rm'] and args[-1].endswith(':decoy')
                                 for args, _, _ in fake.commands.values()))

    def test_seed_collision_blocks_before_first_load(self):
        for collision in ('id', 'reference'):
            fake = Fake()
            refs = lane.references(fake.inputs)
            if collision == 'id':
                selected = image('subject', [refs['source']])
                fake.images[selected['Id']] = selected
            else:
                fake.images[FOREIGN]['RepoTags'] = [lane.fixture.familiar_reference(refs['alias'])]
            with self.assertRaises(ValueError):
                lane._Program(fake).exercise()
            self.assertEqual(fake.load_count, 0)

    def test_config_reference_platform_and_diffid_adversaries(self):
        refs = lane.references(INPUTS)
        original = lane.projection(image('subject', [refs['source']]))
        for key, value in (('Id', FOREIGN), ('Architecture', 'amd64'), ('Os', 'windows'),
                           ('RepoDigests', []), ('RepoTags', ['foreign:tag']),
                           ('RootFS', {'Type': 'layers', 'Layers': []}), ('Created', 'today')):
            row = copy.deepcopy(original)
            row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                lane.owned_image(row, 'subject', [refs['source']])
        for key, value in (('Cmd', ['secret']), ('User', '10001'), ('AttachStdin', 0), ('Unknown', None)):
            row = copy.deepcopy(original)
            row['Config'][key] = value
            with self.assertRaises(ValueError):
                lane.owned_image(row, 'subject', [refs['source']])

    def test_pure_replay_reconstructs_program_not_recorded_operation_list(self):
        fake = Fake()
        source = lane._Program(fake)
        expected = source.exercise()
        expected = source.cleanup()
        def read(output, inputs, environment, index, args, plan):
            actual_args, actual_plan, result = fake.commands[index]
            self.assertEqual(args, actual_args)
            self.assertEqual(plan, actual_plan)
            return result
        def guard(output, inputs, context, engine):
            self.assertIn(context, fake.guards)
            self.assertEqual(engine, context + 1)
            return {'commands': [{'receipt': {'started_unix_ns': index * 10, 'elapsed_ns': 1}}
                                 for index in (context, engine)]}
        with mock.patch.object(lane, '_read_command', side_effect=read), \
             mock.patch.object(lane.commands, 'validate_guard', side_effect=guard), \
             mock.patch.object(lane._Replay, 'finish'), \
             mock.patch.object(lane.driver.Driver, 'command', side_effect=AssertionError('dispatch')):
            actual = lane.replay(Path('/inert/evidence'), INPUTS, environment={'LC_ALL': 'C'}, cleanup=True)
        self.assertEqual(actual, expected)

    def test_dispatch_wrapper_uses_exact_existing_driver_plan_and_raw_readback(self):
        item = SimpleNamespace(inputs=SimpleNamespace(raw=INPUTS), output=Path('/inert/evidence'),
            env={'LC_ALL': 'C'}, record=SimpleNamespace(count=0, max_stream_bytes=lane.driver.MAX_STREAM_BYTES))
        result = SimpleNamespace(index=1, stdout=b'ack', stderr=b'')
        item.command = mock.Mock(return_value=result)
        live = lane._Live(item)
        plan = lane.io_plan(b'fixed-input')
        with mock.patch.object(lane, '_read_command', return_value=result) as reader:
            self.assertIs(live.command(['image', 'load', '--platform', 'linux/arm64'], plan=plan), result)
        item.command.assert_called_once_with(['image', 'load', '--platform', 'linux/arm64'],
                                            expected=0, timeout=30, interaction_plan=plan)
        self.assertEqual(reader.call_args.args[2], item.env)

    def test_live_cleanup_requires_independent_exercise_replay(self):
        value = object.__new__(lane.ImageRoundTrip)
        value._exercise_verified = False
        with self.assertRaises(ValueError):
            value.cleanup()

    def test_source_pinned_load_ack_is_familiar_exact_and_not_duplicated(self):
        program = lane._Program(Fake())
        reference = program.refs['source']
        expected = ('Loaded image: ' + lane.fixture.familiar_reference(reference) + '\n').encode()
        raw = lane.fixture.archive('subject', reference)
        program.mutate = mock.Mock(return_value=SimpleNamespace(stdout=expected))
        program.load('subject', reference, raw)
        for wrong in ((('Loaded image: ' + reference + '\n').encode()), expected * 2, expected[:-1], b''):
            program.mutate.return_value.stdout = wrong
            with self.assertRaises(ValueError):
                program.load('subject', reference, raw)

    def test_source_pinned_removal_ack_and_no_parent_pruning(self):
        program = lane._Program(Fake())
        reference = program.refs['alias']
        expected = lane.fixture.fixture('subject')
        untagged = ('Untagged: ' + lane.fixture.familiar_reference(reference) + '\n').encode()
        deleted = ('Deleted: ' + expected['manifest_digest'] + '\n').encode()
        program.mutate = mock.Mock(return_value=SimpleNamespace(stdout=untagged))
        program.remove('subject', reference, last_reference=False)
        program.mutate.assert_called_with(['image', 'rm', '--no-prune', reference])
        program.mutate.return_value.stdout = untagged + deleted
        program.remove('subject', reference, last_reference=True)
        for wrong in (untagged, untagged + deleted * 2, untagged + b'Deleted: ' + expected['config_digest'].encode() + b'\n',
                      untagged + b'Deleted: ' + FOREIGN.encode() + b'\n'):
            program.mutate.return_value.stdout = wrong
            with self.assertRaises(ValueError):
                program.remove('subject', reference, last_reference=True)
        program.mutate.return_value.stdout = untagged + deleted
        with self.assertRaises(ValueError):
            program.remove('subject', reference, last_reference=False)

    def test_replay_rejects_noninteger_or_reordered_command_windows(self):
        replay = lane._Replay(Path('/inert/evidence'), INPUTS, {})
        replay._time(100, 101)
        for start, end in ((100, 102), (True, 102), (102, 101), (102, 103.0)):
            with self.assertRaises(ValueError):
                replay._time(start, end)

    def test_replay_command_inventory_rejects_extra_or_missing_operations(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            replay = lane._Replay(output, INPUTS, {})
            replay._files(1, True)
            for name in replay.expected_files:
                (output / name).write_bytes(b'')
            replay.finish()
            extra = output / 'command-00002.json'
            extra.write_bytes(b'')
            with self.assertRaises(ValueError):
                replay.finish()
            extra.unlink()
            (output / 'command-00001.interaction-plan.json').unlink()
            with self.assertRaises(ValueError):
                replay.finish()

    def test_inventory_limits_and_refs_bound_to_machine_and_run(self):
        for key in ('RepoTags', 'RepoDigests'):
            for invalid in (False, 0, ''):
                row = image('subject', [lane.references(INPUTS)['source']])
                row[key] = invalid
                with self.assertRaises(ValueError):
                    lane.projection(row)
        oversized = ''.join('sha256:%064x\n' % index for index in range(259)).encode()
        for raw in (b'x\n', oversized, FOREIGN.encode()):
            with self.assertRaises(ValueError):
                lane.image_ids(raw)
        first = lane.references(INPUTS)
        changed = copy.deepcopy(INPUTS)
        changed['scope']['machine_id'] = 'machine-2'
        self.assertNotEqual(first, lane.references(changed))
        self.assertTrue(all(re.fullmatch(r'docker.io/library/vz-image-[0-9a-f]{24}:(source|alias|decoy)', ref)
                            for ref in first.values()))


if __name__ == '__main__':
    unittest.main()
