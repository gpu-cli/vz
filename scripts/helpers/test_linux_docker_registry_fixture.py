"""Synthetic offline input controls, not actual Distribution or Docker proof."""
import copy
import gzip
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import linux_docker_registry_fixture as fixture

OWNER = {'project_id': 'prj_one', 'environment_id': 'env_one', 'machine_id': 'mach_one'}


def layer(payload=b'public synthetic binary', *, compressed=True):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w', format=tarfile.USTAR_FORMAT) as archive:
        item = tarfile.TarInfo('bin/registry')
        item.size, item.mode = len(payload), 0o755
        archive.addfile(item, io.BytesIO(payload))
    raw = output.getvalue()
    return gzip.compress(raw, mtime=0) if compressed else raw, 'sha256:' + fixture.sha(raw)


def descriptor(media, raw):
    return {'mediaType': media, 'digest': 'sha256:' + fixture.sha(raw), 'size': len(raw)}


def synthetic(*, docker=False, variant=True, compressed=True, duplicate_platform=False,
              invalid_config_platform=False, wrong_diff_id=False):
    content, diff_id = layer(compressed=compressed)
    layer_media = ('application/vnd.docker.image.rootfs.diff.tar.gzip' if docker else
                   'application/vnd.oci.image.layer.v1.tar+gzip') if compressed else 'application/vnd.oci.image.layer.v1.tar'
    layer_pin = {**descriptor(layer_media, content), 'diff_id': diff_id}
    if wrong_diff_id:
        layer_pin['diff_id'] = 'sha256:' + 'f' * 64
    platform = {'os': 'linux', 'architecture': 'arm64'}
    if variant:
        platform['variant'] = 'v8'
    config = fixture.canonical({**platform, 'architecture': 'amd64' if invalid_config_platform else 'arm64',
        'config': {'Entrypoint': ['/bin/registry'], 'Cmd': ['serve', '/config.yml']},
        'rootfs': {'type': 'layers', 'diff_ids': [layer_pin['diff_id']]}})
    config_pin = descriptor(fixture.DOCKER_CONFIG if docker else fixture.CONFIG, config)
    manifest = fixture.canonical({'schemaVersion': 2,
        'mediaType': fixture.DOCKER_MANIFEST if docker else fixture.MANIFEST,
        'config': config_pin, 'layers': [{key: layer_pin[key] for key in ('mediaType', 'digest', 'size')}]})
    manifest_pin = descriptor(fixture.DOCKER_MANIFEST if docker else fixture.MANIFEST, manifest)
    selected = {**manifest_pin, 'platform': platform}
    other = {**manifest_pin, 'digest': 'sha256:' + 'a' * 64,
             'platform': {'os': 'linux', 'architecture': 'amd64'}}
    upstream = fixture.canonical({'schemaVersion': 2,
        'mediaType': fixture.DOCKER_INDEX if docker else fixture.INDEX,
        'manifests': [selected, other, selected] if duplicate_platform else [selected, other]})
    index_pin = descriptor(fixture.DOCKER_INDEX if docker else fixture.INDEX, upstream)
    pins = {'schema_version': 1, 'source': {'repository': fixture.REPOSITORY, 'tag': fixture.TAG,
        'index': index_pin, 'platform_manifest': manifest_pin, 'platform': platform},
        'config': config_pin, 'layers': [layer_pin], 'layout_index': {'sha256': '0' * 64, 'size': 1}}
    wrapper = fixture.canonical(fixture.selected_index(pins))
    pins['layout_index'] = {'sha256': fixture.sha(wrapper), 'size': len(wrapper)}
    files = {'oci-layout': fixture.canonical({'imageLayoutVersion': '1.0.0'}), 'index.json': wrapper}
    for pin, raw in ((index_pin, upstream), (manifest_pin, manifest), (config_pin, config), (layer_pin, content)):
        files[fixture.blob_path(pin['digest'])] = raw
    return pins, files


class RegistryFixtureTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='vz-registry-fixture-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def layout(self, **options):
        pins, files = synthetic(**options)
        root = self.root / ('layout-' + str(len(list(self.root.iterdir()))))
        (root / 'blobs/sha256').mkdir(parents=True)
        for path, raw in files.items():
            (root / path).write_bytes(raw)
        return root, pins

    def test_oci_and_docker_metadata_compressed_and_plain_layers(self):
        for docker, compressed, variant in ((False, True, True), (True, True, False), (False, False, False)):
            with self.subTest(docker=docker, compressed=compressed):
                root, pins = self.layout(docker=docker, compressed=compressed, variant=variant)
                proof = fixture.validate_layout(root, pins=pins)
                self.assertEqual(proof['manifest_digest'], pins['source']['platform_manifest']['digest'])
                self.assertEqual(proof['layers'][0]['diff_id'], pins['layers'][0]['diff_id'])
                self.assertEqual(proof['layers'][0]['members'], 1)
                self.assertFalse(proof['registry_execution_certified'])
                self.assertFalse(proof['docker_load_certified'])
                self.assertFalse(proof['source_authenticity_certified'])
                self.assertFalse(proof['upstream_other_platforms_included'])
                self.assertEqual(fixture.validate_layout(root, pins=pins), proof)

    def test_no_dispatch_or_write_during_admission(self):
        root, pins = self.layout()
        original_open = os.open

        def readonly_open(path, flags, *args, **kwargs):
            self.assertFalse(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            return original_open(path, flags, *args, **kwargs)

        with mock.patch('subprocess.Popen', side_effect=AssertionError('dispatch')), \
             mock.patch('socket.socket', side_effect=AssertionError('network')), \
             mock.patch('os.open', side_effect=readonly_open):
            fixture.validate_layout(root, pins=pins)

    def test_external_pins_are_not_replaced_by_artifact_observations(self):
        root, pins = self.layout()
        for route, key, value in ((pins['source'], 'tag', 'latest'),
                                  (pins['source'], 'repository', 'foreign/registry'),
                                  (pins['config'], 'size', True),
                                  (pins['layout_index'], 'sha256', 'f' * 64)):
            old = route[key]
            route[key] = value
            with self.assertRaises(fixture.FixtureError):
                fixture.validate_layout(root, pins=pins)
            route[key] = old
        changed = copy.deepcopy(pins)
        changed['layers'][0]['urls'] = ['https://foreign.invalid/layer']
        with self.assertRaises(fixture.FixtureError):
            fixture.validate_layout(root, pins=changed)

    def test_tampered_each_blob_and_wrapper_rejected(self):
        for relative in synthetic()[1]:
            root, pins = self.layout()
            path = root / relative
            path.write_bytes(path.read_bytes() + b' ')
            with self.subTest(relative=relative), self.assertRaises(fixture.FixtureError):
                fixture.validate_layout(root, pins=pins)

    def test_missing_extra_unselected_and_wrong_directory_inventory(self):
        for mode in ('missing', 'extra', 'unselected', 'directory'):
            root, pins = self.layout()
            if mode == 'missing':
                (root / fixture.blob_path(pins['layers'][0]['digest'])).unlink()
            elif mode == 'directory':
                (root / 'extra').mkdir()
            else:
                name = 'unexpected.json' if mode == 'extra' else fixture.blob_path('sha256:' + 'a' * 64)
                (root / name).write_bytes(b'{}')
            with self.subTest(mode=mode), self.assertRaises(fixture.FixtureError):
                fixture.validate_layout(root, pins=pins)

    def test_symlink_hardlink_fifo_and_writable_blob_rejected(self):
        for kind in ('symlink', 'hardlink', 'fifo', 'mode'):
            root, pins = self.layout()
            path = root / fixture.blob_path(pins['layers'][0]['digest'])
            if kind == 'mode':
                path.chmod(0o666)
            else:
                other = self.root / ('external-' + kind)
                path.rename(other)
                if kind == 'symlink':
                    path.symlink_to(other)
                elif kind == 'hardlink':
                    os.link(other, path)
                else:
                    os.mkfifo(path, 0o600)
            with self.subTest(kind=kind), self.assertRaises(fixture.FixtureError):
                fixture.validate_layout(root, pins=pins)

    def test_duplicate_selection_wrong_platform_and_bad_diff_id_rejected(self):
        for options in ({'duplicate_platform': True}, {'invalid_config_platform': True}, {'wrong_diff_id': True}):
            root, pins = self.layout(**options)
            with self.subTest(options=options), self.assertRaises(fixture.FixtureError):
                fixture.validate_layout(root, pins=pins)

    def test_json_rejects_duplicates_nonfinite_and_malformed_without_data_echo(self):
        for raw in (b'{"private-secret":1,"private-secret":2}', b'{"x":NaN}', b'private-secret', b'[]' * 600000):
            with self.assertRaises(fixture.FixtureError) as caught:
                fixture.decode(raw)
            self.assertNotIn('private-secret', str(caught.exception))

    def test_resource_names_bind_all_owners_and_run_but_authority_is_same(self):
        original = fixture.resource_spec(OWNER, 'run-one')
        values = [fixture.resource_spec(dict(OWNER, **{key: value + '-other'}), 'run-one')
                  for key, value in OWNER.items()]
        values.append(fixture.resource_spec(OWNER, 'run-two'))
        self.assertEqual(len({item['container_name'] for item in [original, *values]}), 5)
        for item in [original, *values]:
            self.assertEqual(item['authority'], original['authority'])
            self.assertTrue(item['internal_network'])
            self.assertEqual(item['published_ports'], [])
            self.assertFalse(item['machine_binding_certified'])
        for owner, run in ((dict(OWNER, extra='x'), 'run'), (dict(OWNER, machine_id='../bad'), 'run'),
                           (OWNER, ''), (OWNER, True)):
            with self.assertRaises(fixture.FixtureError):
                fixture.resource_spec(owner, run)

    def tls(self):
        spec = fixture.resource_spec(OWNER, 'run-one')
        expected = {'ca_sha256': 'a' * 64, 'certificate_sha256': 'b' * 64, 'spki_sha256': 'c' * 64}
        now = 1700000000 * 10**9
        metadata = {'schema_version': 1, 'owner': dict(OWNER), 'run_id': 'run-one', 'authority': spec['authority'],
            **expected, 'issuer_ca_sha256': expected['ca_sha256'], 'san_ips': [spec['address']], 'san_dns': [],
            'is_ca': False, 'ca_is_ca': True, 'key_usage': ['digital_signature'], 'extended_key_usage': ['server_auth'],
            'not_before_unix_ns': now - 10**9, 'not_after_unix_ns': now + 3600 * 10**9}
        return spec, expected, metadata, now

    def test_tls_metadata_binds_pins_and_scope_without_signature_claim(self):
        spec, expected, metadata, now = self.tls()
        proof = fixture.validate_tls_public(metadata, spec=spec, expected=expected, observed_unix_ns=now)
        self.assertEqual(proof['metadata_sha256'], fixture.sha(fixture.canonical(metadata)))
        self.assertFalse(proof['certificate_chain_verified'])
        self.assertFalse(proof['handshake_certified'])

    def test_tls_foreign_owner_ca_san_usage_expiry_and_secret_fields_rejected(self):
        spec, expected, original, now = self.tls()
        for key, value in (('owner', dict(OWNER, machine_id='foreign')), ('run_id', 'other'),
                           ('authority', '127.0.0.1:5443'), ('san_ips', ['127.0.0.1']),
                           ('san_dns', ['*.invalid']), ('issuer_ca_sha256', 'd' * 64),
                           ('certificate_sha256', 'd' * 64), ('is_ca', True), ('ca_is_ca', 1),
                           ('extended_key_usage', ['client_auth']), ('not_after_unix_ns', now),
                           ('not_before_unix_ns', now + 1), ('schema_version', True),
                           ('private_key', 'private-secret')):
            metadata = dict(original, **{key: value})
            with self.subTest(key=key), self.assertRaises(fixture.FixtureError) as caught:
                fixture.validate_tls_public(metadata, spec=spec, expected=expected, observed_unix_ns=now)
            self.assertNotIn('private-secret', str(caught.exception))

    def test_changed_tree_between_layer_scan_and_final_inventory_rejected(self):
        root, pins = self.layout()
        original = fixture.artifact.scan_layer

        def change(*args, **kwargs):
            proof = original(*args, **kwargs)
            (root / 'index.json').write_bytes(b'{}')
            return proof

        with mock.patch.object(fixture.artifact, 'scan_layer', side_effect=change), \
             self.assertRaises(fixture.FixtureError):
            fixture.validate_layout(root, pins=pins)


if __name__ == '__main__':
    unittest.main()
