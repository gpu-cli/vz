"""Pure image bytes/reference controls, never registry or Docker dispatch."""
import copy
import io
import tarfile
import unittest

import linux_docker_registry_image as subject


class ImageTests(unittest.TestCase):
    def setUp(self):
        self.spec = subject.registry.resource_spec({'project_id': 'prj_one', 'environment_id': 'env_one',
                                                   'machine_id': 'mch_one'}, 'run_one')
        self.recipe = subject.contract(self.spec)
        self.expected = self.recipe['expected']

    def row(self, stage):
        tags, digests = subject._references(self.recipe, stage)
        return {'Id': self.expected['manifest_digest'], 'Architecture': 'arm64', 'Os': 'linux',
                'Created': subject.image.CREATED, 'RootFS': {'Type': 'layers', 'Layers': [self.expected['diff_id']]},
                'Config': copy.deepcopy(self.expected['config']['config']), 'RepoTags': tags, 'RepoDigests': digests}

    def validate(self, row, stage='loaded'):
        return subject.validate_inspect(subject.image.canonical([row]), spec=self.spec, stage=stage)

    def blobs(self):
        raw = subject.seed(self.spec)
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as tar:
            return {kind: tar.extractfile('blobs/sha256/' + self.expected[kind + '_digest'][7:]).read()
                    for kind in ('manifest', 'config', 'layer')}

    def test_source_commands_and_distinct_owner_references(self):
        self.assertEqual(self.recipe['remote_reference'], self.spec['repository'] + ':subject')
        self.assertEqual(self.recipe['commands']['push'], ['push', self.recipe['remote_reference']])
        self.assertEqual(self.recipe['commands']['pull_digest'][-1], self.recipe['digest_reference'])
        self.assertNotIn('--force', repr(self.recipe['commands']))
        self.assertEqual(self.recipe['seed_reference'], self.recipe['export_reference'])
        other = subject.registry.resource_spec(self.spec['owner'], 'run_two')
        self.assertNotEqual(subject.contract(other)['seed_reference'], self.recipe['seed_reference'])
        self.assertEqual(subject.contract(other)['expected'], self.expected)

    def test_actual_seed_export_and_remote_bytes(self):
        proof = subject.validate_export(subject.seed(self.spec), spec=self.spec)
        self.assertTrue(proof)
        blobs = self.blobs()
        proof = subject.validate_remote(**blobs)
        self.assertEqual(proof['manifest_digest'], self.expected['manifest_digest'])
        self.assertFalse(proof['remote_transport_authenticated'])
        self.assertFalse(proof['registry_execution_certified'])

    def test_all_inspect_stages_and_digest_name_in_repotags(self):
        for stage in ('loaded', 'tagged', 'pulled', 'export-tagged'):
            proof = self.validate(self.row(stage), stage)
            self.assertFalse(proof['config_digest_certified'])
        self.assertEqual(self.row('pulled')['RepoTags'], [self.recipe['digest_reference']])
        row = self.row('pulled'); row['RepoTags'] = []
        with self.assertRaises(ValueError): self.validate(row, 'pulled')

    def test_wrong_subject_platform_config_and_reference_rejected(self):
        changes = [('Id', subject.image.fixture('decoy')['manifest_digest']), ('Id', self.expected['config_digest']),
                   ('Architecture', 'amd64'), ('Os', 'windows'), ('Variant', 'v9'), ('Created', 'later'),
                   ('RootFS', {'Type': 'layers', 'Layers': []}), ('RepoTags', []),
                   ('RepoDigests', ['foreign@' + self.expected['manifest_digest']])]
        for key, value in changes:
            row = self.row('loaded'); row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(row)
        for key, value in (('Labels', {}), ('WorkingDir', '/other'), ('Env', ['PRIVATE=x']),
                           ('User', 'root'), ('Tty', 0), ('Unknown', None)):
            row = self.row('loaded'); row['Config'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.validate(row)

    def test_raw_schema_duplicates_and_extra_reference_rejected(self):
        with self.assertRaises(ValueError):
            subject.validate_inspect(b'[{"Id":1,"Id":2}]', spec=self.spec, stage='loaded')
        row = self.row('loaded'); row['RepoTags'] *= 2
        with self.assertRaises(ValueError): self.validate(row)
        with self.assertRaises(ValueError): self.validate(self.row('loaded'), 'caller_chosen')

    def test_each_remote_blob_tamper_and_reserialized_manifest_rejected(self):
        for key in ('manifest', 'config', 'layer'):
            blobs = self.blobs(); blobs[key] += b' '
            with self.assertRaises(ValueError): subject.validate_remote(**blobs)
        blobs = self.blobs(); blobs['manifest'] = blobs['manifest'].rstrip()
        with self.assertRaises(ValueError): subject.validate_remote(**blobs)

    def test_export_foreign_reference_decoy_or_extra_bytes_rejected(self):
        for raw in (subject.image.archive('decoy', self.recipe['export_reference']),
                    subject.image.archive('subject', 'docker.io/library/foreign:subject'),
                    subject.seed(self.spec) + b'foreign'):
            with self.assertRaises(ValueError): subject.validate_export(raw, spec=self.spec)

    def test_absence_bound_and_foreign_spec(self):
        raw = (subject.image.fixture('decoy')['manifest_digest'] + '\n').encode()
        self.assertTrue(subject.validate_absent(raw, spec=self.spec)['subject_manifest_absent'])
        for bad in ((self.expected['manifest_digest'] + '\n').encode(), raw.rstrip(), b'not-an-id\n', b'x' * 65537):
            with self.assertRaises(ValueError): subject.validate_absent(bad, spec=self.spec)
        for key, value in (('repository', 'foreign/repo'), ('address', '127.0.0.1'), ('published_ports', [5443])):
            spec = copy.deepcopy(self.spec); spec[key] = value
            with self.assertRaises(ValueError): subject.contract(spec)


if __name__ == '__main__':
    unittest.main()
