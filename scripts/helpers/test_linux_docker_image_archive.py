import copy
import hashlib
import json
import tarfile
import unittest

import linux_docker_image_archive as subject
import linux_docker_image_fixture as fixture


REF = 'docker.io/library/vz-image-0123456789abcdef01234567:alias'


def encoded(value):
    return json.dumps(value, separators=(',', ':')).encode()


def archive(entries):
    output = bytearray()
    for name, content, properties in entries:
        header = tarfile.TarInfo(name)
        header.mode = 0o644
        header.size = len(content)
        for key, value in properties.items():
            setattr(header, key, value)
        output.extend(header.tobuf(format=tarfile.USTAR_FORMAT))
        output.extend(content)
        output.extend(bytes(-len(content) % 512))
    return bytes(output) + bytes(1024)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        identity = fixture.fixture('subject')
        self.kw = {key: identity[key[9:]] for key in (
            'expected_manifest_digest', 'expected_config_digest', 'expected_layer_digest',
            'expected_diff_id', 'expected_labels')}
        self.kw.update(expected_reference=REF, expected_payload_path='/payload.txt',
                       expected_payload_sha256=identity['payload']['sha256'],
                       expected_payload_size=identity['payload']['size'])
        self.raw = fixture.archive('subject', REF)
        self.files = subject._tar(self.raw, outer=True)[0]

    def pack(self, files=None):
        return archive([(name, value, {}) for name, value in
                        (self.files if files is None else files).items()])

    def reject(self, raw=None, **kwargs):
        expected = dict(self.kw, **kwargs)
        with self.assertRaises(ValueError):
            subject.validate(self.pack() if raw is None else raw, **expected)

    def mutate_json(self, path, change):
        value = json.loads(self.files[path])
        change(value)
        self.files[path] = encoded(value)

    def rebind(self, *, layer=None, config_change=None, manifest_change=None):
        """Reseal blob identities so semantic tests do not just trip digest checks."""
        manifest_path = 'blobs/sha256/' + self.kw['expected_manifest_digest'][7:]
        config_path = 'blobs/sha256/' + self.kw['expected_config_digest'][7:]
        layer_path = 'blobs/sha256/' + self.kw['expected_layer_digest'][7:]
        manifest = json.loads(self.files.pop(manifest_path))
        config = json.loads(self.files.pop(config_path))
        old_layer = self.files.pop(layer_path)
        layer_raw = old_layer if layer is None else layer
        new_layer = 'sha256:' + hashlib.sha256(layer_raw).hexdigest()
        self.kw['expected_layer_digest'] = self.kw['expected_diff_id'] = new_layer
        config['rootfs']['diff_ids'] = [new_layer]
        if config_change:
            config_change(config)
        config_raw = encoded(config)
        new_config = 'sha256:' + hashlib.sha256(config_raw).hexdigest()
        self.kw['expected_config_digest'] = new_config
        manifest['config'].update(digest=new_config, size=len(config_raw))
        manifest['layers'][0].update(digest=new_layer, size=len(layer_raw))
        if manifest_change:
            manifest_change(manifest)
        manifest_raw = encoded(manifest)
        new_manifest = 'sha256:' + hashlib.sha256(manifest_raw).hexdigest()
        self.kw['expected_manifest_digest'] = new_manifest
        for digest, raw in ((new_manifest, manifest_raw), (new_config, config_raw), (new_layer, layer_raw)):
            self.files['blobs/sha256/' + digest[7:]] = raw
        self.mutate_json('index.json', lambda value: value['manifests'][0].update(
            digest=new_manifest, size=len(manifest_raw)))
        self.files['manifest.json'] = encoded([{'Config': 'blobs/sha256/' + new_config[7:],
            'RepoTags': [REF.removeprefix('docker.io/library/')],
            'Layers': ['blobs/sha256/' + new_layer[7:]]}])

    def test_seed_and_familiar_reference(self):
        for reference in (REF, REF[len('docker.io/library/'):]):
            proof = subject.validate(self.raw, **dict(self.kw, expected_reference=reference))
            self.assertEqual(proof['archive_bytes'], 8704)
            self.assertFalse(proof['machine_binding_certified'])
            self.assertFalse(proof['docker_round_trip_certified'])

    def test_exporter_headers_directories_and_zero_padding(self):
        entries = [('blobs/', b'', {'type': tarfile.DIRTYPE, 'mode': 0o755}),
                   ('blobs/sha256/', b'', {'type': tarfile.DIRTYPE, 'mode': 0o755})]
        entries += [(name, value, {'mode': 0o644 if name in ('index.json', 'manifest.json') else 0o444})
                    for name, value in self.files.items()]
        self.mutate_json('index.json', lambda value: value['manifests'][0].pop('platform'))
        entries = entries[:2] + [(name, value, properties) if name != 'index.json' else
                    (name, self.files[name], properties) for name, value, properties in entries[2:]]
        proof = subject.validate(archive(entries) + bytes(1024), **self.kw)
        self.assertEqual(proof['directory_members'], 2)

    def test_bounds_types_truncation_trailing(self):
        for raw in (bytearray(self.raw), b'', self.raw[:-1], self.raw[:-1024],
                    self.raw + b'x' + bytes(511), self.raw + bytes(subject.LIMIT)):
            with self.subTest(size=len(raw)):
                self.reject(raw)

    def test_bad_checksum_and_nonzero_padding(self):
        damaged = bytearray(self.raw)
        damaged[0] ^= 1
        self.reject(bytes(damaged))
        damaged = bytearray(self.raw)
        size = tarfile.TarInfo.frombuf(damaged[:512], 'ascii', 'strict').size
        damaged[512 + size] = 1
        self.reject(bytes(damaged))

    def test_duplicate_and_extra_inventory(self):
        entries = [(name, value, {}) for name, value in self.files.items()]
        for extra in (entries[0], ('extra', b'x', {}),
                      ('blobs/sha256/' + 'f' * 64, b'x', {})):
            self.reject(archive(entries + [extra]))

    def test_tar_path_and_header_extensions(self):
        for path in ('/absolute', '../escape', 'a/../escape', 'a//b', './oci-layout'):
            self.reject(archive([(path, b'x', {})]))
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.XHDTYPE,
                     tarfile.XGLTYPE, tarfile.GNUTYPE_SPARSE, tarfile.CHRTYPE):
            self.reject(archive([('oci-layout', b'x', {'type': kind})]))

    def test_ownership_modes_and_directory(self):
        entries = [(name, value, {}) for name, value in self.files.items()]
        for properties in ({'uid': 1}, {'gid': 1}, {'mode': 0o4755}, {'mtime': 1},
                           {'uname': 'root'}, {'linkname': 'elsewhere'}):
            self.reject(archive([(entries[0][0], entries[0][1], properties)] + entries[1:]))
        self.reject(archive(entries + [('elsewhere/', b'', {'type': tarfile.DIRTYPE, 'mode': 0o755})]))

    def test_duplicate_json_and_constants(self):
        for raw in (b'{"imageLayoutVersion":"1.0.0","imageLayoutVersion":"1.0.0"}',
                    b'{"imageLayoutVersion":NaN}', b'[' * 2000 + b']' * 2000):
            self.files['oci-layout'] = raw
            self.reject()

    def test_descriptor_extra_images_and_platform(self):
        original = copy.deepcopy(self.files)
        for change in (lambda i: i['manifests'].append(copy.deepcopy(i['manifests'][0])),
                       lambda i: i['manifests'][0].update(size=True),
                       lambda i: i['manifests'][0].update(urls=['https://elsewhere']),
                       lambda i: i['manifests'][0]['platform'].update(architecture='amd64'),
                       lambda i: i.update(schemaVersion=True)):
            self.files = copy.deepcopy(original)
            self.mutate_json('index.json', change)
            self.reject()

    def test_annotations_and_compatibility_drift(self):
        original = copy.deepcopy(self.files)
        for change in (lambda i: i['manifests'][0].pop('annotations'),
                       lambda i: i['manifests'][0]['annotations'].update({'org.opencontainers.image.ref.name': REF}),
                       lambda i: i['manifests'][0]['annotations'].update({'extra': 'untrusted'})):
            self.files = copy.deepcopy(original)
            self.mutate_json('index.json', change)
            self.reject()
        self.files = original
        self.mutate_json('manifest.json', lambda rows: rows[0].update(RepoTags=[REF]))
        self.reject()

    def test_digests_and_expectations(self):
        self.reject(expected_manifest_digest='sha256:' + '0' * 64)
        self.reject(expected_diff_id='sha256:' + '0' * 64)
        self.reject(expected_payload_sha256='0' * 64)
        self.reject(expected_payload_size=True)
        self.reject(expected_reference='other:alias')
        self.reject(expected_payload_path='//payload.txt')
        self.reject(expected_labels={'different': 'label'})

    def test_resealed_config_platform_rootfs_and_runtime(self):
        for change in (lambda c: c.update(architecture='amd64'),
                       lambda c: c['rootfs'].update(type='other'),
                       lambda c: c['config'].update(Cmd=['evil']),
                       lambda c: c['config'].update(Labels={'foreign': 'label'}),
                       lambda c: c['history'].append(copy.deepcopy(c['history'][0]))):
            self.setUp()
            self.rebind(config_change=change)
            self.reject()

    def test_resealed_manifest_unknown_types_and_graph(self):
        for change in (lambda m: m['layers'][0].update(mediaType=subject.LAYER + '+gzip'),
                       lambda m: m.update(subject={'digest': 'sha256:' + '0' * 64}),
                       lambda m: m['layers'].append(copy.deepcopy(m['layers'][0]))):
            self.setUp()
            self.rebind(manifest_change=change)
            self.reject()

    def test_resealed_layer_extra_link_wrong_payload(self):
        payload = b'vz04-image-fixture-v1\nrole=subject\n'
        for entries in ([('payload.txt', payload, {}), ('extra', b'x', {})],
                        [('payload.txt', payload, {'mode': 0o755})],
                        [('payload.txt', b'different', {})],
                        [('payload.txt', b'', {'type': tarfile.SYMTYPE, 'linkname': 'elsewhere'})]):
            self.setUp()
            self.rebind(layer=archive(entries))
            self.reject()

    def test_resealed_same_semantics_pass(self):
        self.rebind()
        self.assertEqual(subject.validate(self.pack(), **self.kw)['payload_bytes'],
                         self.kw['expected_payload_size'])


if __name__ == '__main__':
    unittest.main()
