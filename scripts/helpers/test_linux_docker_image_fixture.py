"""Exact deterministic seed bytes; no Docker, registry, image load or extraction."""
import copy
import io
import json
import os
import tarfile
import unittest
from unittest import mock

import linux_docker_image_fixture as fixture

REFERENCE = 'docker.io/library/vz-image-test:'
KNOWN = {
    'subject': {'archive': '915c8b6070b8116319867c81e09e3d6a32903d3838ea91ec84bbfc00d48accf2',
                'manifest': '06b51e395dfc50d310f51b4971292678ba38b2ce30a27b6ec368bc65e82bc397',
                'config': '9175c198a309d506d21a6b34a948096f4fe1154a345a1ec6873269165beea40c',
                'layer': '3c95f7c76a8b91c45c470d83bce1659f055fb01728c7e2a651084c11ea0565ed',
                'payload': 'c9f49530909ec87b470f94c3857145ac175443644ea9f9d096a7744c1fc617bf'},
    'decoy': {'archive': '35d73df94d6279967d11a6e8546cae41c03bed40c8111a5d1198d01dc45f58a8',
              'manifest': '4f3fb1693110442bf3b1add552f6c96937edbe97714e4b01370d48ec3807f26c',
              'config': 'a8ff0e2e97d5e434fe4d60fed8f226d11a5fff9410e0b7b448a5c7193085f554',
              'layer': '4b7c8124768827922bb75b59d89f7c6f1906f8f03b5101899b721c499d43789a',
              'payload': '9954ec2291151503996be221483d14094ad485c5e4e20b012662e1ddb9298b04'},
}


def inventory(raw):
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as reader:
        for member in reader:
            assert member.isreg() and member.name not in result
            result[member.name] = (member, reader.extractfile(member).read())
    return result


class ImageFixtureTests(unittest.TestCase):
    def test_entire_archive_and_every_known_content_digest(self):
        for role, known in KNOWN.items():
            raw = fixture.archive(role, REFERENCE + role)
            expected = fixture.fixture(role)
            self.assertEqual(len(raw), 8704)
            self.assertLessEqual(len(raw), fixture.LIMIT)
            self.assertEqual(fixture.sha256(raw), known['archive'])
            for kind in ('manifest', 'config', 'layer'):
                self.assertEqual(expected[kind + '_digest'], 'sha256:' + known[kind])
            self.assertEqual(expected['diff_id'], expected['layer_digest'])
            self.assertEqual(expected['payload']['sha256'], known['payload'])
            self.assertEqual(raw, fixture.archive(role, REFERENCE + role))

    def test_ustar_regular_zero_metadata_and_exact_complete_inventory(self):
        for role in fixture.ROLES:
            raw = fixture.archive(role, REFERENCE + role)
            entries = inventory(raw)
            expected = fixture.fixture(role)
            self.assertEqual(set(entries), {'oci-layout', 'index.json', 'manifest.json'} |
                             {'blobs/sha256/' + expected[name + '_digest'][7:]
                              for name in ('manifest', 'config', 'layer')})
            self.assertEqual(list(entries), sorted(entries))
            for member, content in entries.values():
                self.assertEqual((member.mode, member.uid, member.gid, member.mtime), (0o644, 0, 0, 0))
                self.assertEqual((member.uname, member.gname, member.linkname, member.pax_headers), ('', '', '', {}))
                self.assertEqual(raw[member.offset + 257:member.offset + 265], b'ustar\x0000')
                self.assertEqual(member.size, len(content))
            last = next(reversed(entries.values()))[0]
            end = last.offset_data + ((last.size + 511) // 512) * 512
            self.assertEqual(raw[end:], b'\0' * 1024)

    def test_descriptors_config_platform_and_payload_cross_bind(self):
        for role in fixture.ROLES:
            expected = fixture.fixture(role)
            entries = {key: value[1] for key, value in inventory(fixture.archive(role, REFERENCE + role)).items()}
            manifest = json.loads(entries['blobs/sha256/' + expected['manifest_digest'][7:]])
            config = json.loads(entries['blobs/sha256/' + expected['config_digest'][7:]])
            self.assertEqual(manifest, expected['manifest'])
            self.assertEqual(config, expected['config'])
            self.assertEqual((config['architecture'], config['os']), ('arm64', 'linux'))
            self.assertEqual(config['config'], {'Labels': expected['labels'], 'WorkingDir': '/'})
            self.assertEqual(config['rootfs'], {'type': 'layers', 'diff_ids': [expected['diff_id']]})
            self.assertEqual(config['created'], '1970-01-01T00:00:00Z')
            self.assertEqual(len(config['history']), 1)
            self.assertEqual(len(manifest['layers']), 1)
            self.assertEqual(manifest['layers'][0]['mediaType'], fixture.LAYER_TYPE)
            for descriptor in [manifest['config'], *manifest['layers']]:
                blob = entries['blobs/sha256/' + descriptor['digest'][7:]]
                self.assertEqual(descriptor['size'], len(blob))
                self.assertEqual(descriptor['digest'], 'sha256:' + fixture.sha256(blob))
            layer = entries['blobs/sha256/' + expected['layer_digest'][7:]]
            payloads = inventory(layer)
            self.assertEqual(list(payloads), ['payload.txt'])
            member, payload = payloads['payload.txt']
            self.assertEqual(payload, ('vz04-image-fixture-v1\nrole=' + role + '\n').encode())
            self.assertEqual((member.mode, member.uid, member.gid, member.mtime), (0o644, 0, 0, 0))
            self.assertEqual(expected['payload']['size'], len(payload))
            self.assertEqual(expected['payload']['sha256'], fixture.sha256(payload))

    def test_reference_changes_only_naming_documents(self):
        first_ref, second_ref = REFERENCE + 'subject', 'docker.io/library/vz-image-other:renamed'
        first = inventory(fixture.archive('subject', first_ref))
        second = inventory(fixture.archive('subject', second_ref))
        self.assertEqual(set(first), set(second))
        self.assertEqual({name for name in first if first[name][1] != second[name][1]}, {'index.json', 'manifest.json'})
        for reference, entries in ((first_ref, first), (second_ref, second)):
            index = json.loads(entries['index.json'][1])
            descriptor = index['manifests'][0]
            self.assertEqual(descriptor['annotations'], {'io.containerd.image.name': reference,
                'org.opencontainers.image.ref.name': reference.rsplit(':', 1)[1]})
            self.assertEqual(descriptor['platform'], {'os': 'linux', 'architecture': 'arm64'})
            self.assertEqual(json.loads(entries['manifest.json'][1]), [{
                'Config': 'blobs/sha256/' + KNOWN['subject']['config'],
                'RepoTags': [fixture.familiar_reference(reference)],
                'Layers': ['blobs/sha256/' + KNOWN['subject']['layer']]}])
            self.assertEqual(json.loads(entries['oci-layout'][1]), {'imageLayoutVersion': '1.0.0'})

    def test_subject_decoy_are_distinct_at_every_content_level(self):
        subject, decoy = [fixture.fixture(role) for role in fixture.ROLES]
        for key in ('manifest_digest', 'config_digest', 'layer_digest', 'diff_id', 'labels'):
            self.assertNotEqual(subject[key], decoy[key])
        self.assertNotEqual(subject['payload']['sha256'], decoy['payload']['sha256'])

    def test_invalid_roles_and_references_rejected_without_echoing_input(self):
        for role in ('', 'Subject', '../subject', True, None):
            with self.assertRaises(ValueError):
                fixture.fixture(role)
        for reference in ('vz-image-test:subject', 'docker.io/library/vz-image-test',
                          'docker.io/library/vz-image-test@sha256:' + 'a' * 64,
                          'docker.io/library/../escape:tag', 'docker.io/library/name:tag\n',
                          'docker.io/library/name:tag;secret', 'docker.io/library/NAME:tag',
                          'docker.io/library/name:UPPER', 'docker.io/library/name:tag/extra',
                          'docker.io/library/' + 'a' * 101 + ':tag',
                          'docker.io/library/name:' + 'a' * 129, None, True):
            with self.subTest(reference=reference), self.assertRaises(ValueError) as caught:
                fixture.archive('subject', reference)
            self.assertNotIn('secret', str(caught.exception))

    def test_returned_identity_is_fresh_and_ambient_state_is_unused(self):
        original = copy.deepcopy(fixture.fixture('subject'))
        mutated = fixture.fixture('subject')
        mutated['labels']['com.vz.fixture.role'] = 'foreign'
        mutated['config']['rootfs']['diff_ids'][0] = 'wrong'
        with mock.patch.dict(os.environ, {'SOURCE_DATE_EPOCH': '99999999', 'TZ': 'Antarctica/Troll'}), \
             mock.patch('time.time', side_effect=AssertionError('ambient clock')), \
             mock.patch('os.urandom', side_effect=AssertionError('random')), \
             mock.patch('builtins.open', side_effect=AssertionError('file I/O')):
            self.assertEqual(fixture.fixture('subject'), original)
            self.assertEqual(fixture.sha256(fixture.archive('subject', REFERENCE + 'subject')), KNOWN['subject']['archive'])


if __name__ == '__main__':
    unittest.main()
