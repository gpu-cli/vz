import io
import tempfile
from pathlib import Path
import unittest
import urllib.error
from unittest import mock

import linux_docker_registry_acquire as subject


class FakePublicRegistry:
    def __init__(self):
        def pin(raw, media):
            return {'mediaType': media, 'digest': 'sha256:' + subject.sha(raw), 'size': len(raw)}
        self.layer = b'public layer bytes (acquisition does not certify diff-ID)'
        layer = pin(self.layer, 'application/vnd.oci.image.layer.v1.tar')
        config = subject.canonical({'os': 'linux', 'architecture': 'arm64', 'rootfs':
            {'type': 'layers', 'diff_ids': ['sha256:' + subject.sha(self.layer)]}})
        config_pin = pin(config, 'application/vnd.oci.image.config.v1+json')
        manifest = subject.canonical({'schemaVersion': 2, 'mediaType': subject.MANIFEST,
                                     'config': config_pin, 'layers': [layer]})
        selected = {**pin(manifest, subject.MANIFEST),
                    'platform': {'os': 'linux', 'architecture': 'arm64', 'variant': 'v8'}}
        self.index = subject.canonical({'schemaVersion': 2, 'mediaType': subject.INDEX,
                                       'manifests': [selected]})
        self.values = {'manifests/' + subject.VERSION: self.index,
                      'manifests/sha256:' + subject.sha(self.index): self.index,
                      'manifests/' + selected['digest']: manifest,
                      'blobs/' + config_pin['digest']: config,
                      'blobs/' + layer['digest']: self.layer}
        self.calls = []

    def get(self, url, **options):
        self.calls.append((url, options))
        raw = self.values[url.removeprefix(subject.REGISTRY)]
        return raw, {'status': 200, 'bytes': len(raw), 'sha256': subject.sha(raw),
                     'docker_content_digest': 'sha256:' + subject.sha(raw)}


class AcquireTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def test_resolve_and_immutable_replay_same_layout(self):
        first = FakePublicRegistry()
        pin = subject.acquire(self.root / 'first', transport=first)
        second = FakePublicRegistry()
        self.assertEqual(pin, subject.acquire(self.root / 'second', pins=pin, transport=second))
        self.assertEqual(second.calls[0][0], subject.REGISTRY + 'manifests/' + pin['source']['index']['digest'])
        self.assertNotIn(subject.REGISTRY + 'manifests/' + subject.VERSION, [x[0] for x in second.calls])
        for name in ('index.json', 'oci-layout'):
            self.assertEqual((self.root / 'first/layout' / name).read_bytes(),
                             (self.root / 'second/layout' / name).read_bytes())
        self.assertEqual(len(list((self.root / 'first/layout/blobs/sha256').iterdir())), 4)
        receipt = subject.parse((self.root / 'second/acquisition.json').read_bytes())
        self.assertFalse(receipt['docker_dispatched'])
        self.assertFalse(receipt['user_credentials_used'])
        self.assertEqual(receipt['request_retries'], 0)

    def test_wrong_frozen_digest_rejected_before_children(self):
        pin = subject.acquire(self.root / 'first', transport=FakePublicRegistry())
        transport = FakePublicRegistry()
        transport.values['manifests/' + pin['source']['index']['digest']] += b' '
        with self.assertRaisesRegex(ValueError, 'content mismatch'):
            subject.acquire(self.root / 'bad', pins=pin, transport=transport)
        self.assertEqual(len(transport.calls), 1)

    def test_existing_destination_never_overwritten(self):
        output = self.root / 'exists'
        output.mkdir()
        transport = FakePublicRegistry()
        with self.assertRaisesRegex(ValueError, 'fresh canonical'):
            subject.acquire(output, transport=transport)
        self.assertEqual(transport.calls, [])

    def test_destination_symlink_rejected(self):
        output = self.root / 'link'
        output.symlink_to(self.root / 'missing')
        with self.assertRaisesRegex(ValueError, 'fresh canonical'):
            subject.acquire(output, transport=FakePublicRegistry())

    def test_external_layers_and_duplicate_json_rejected(self):
        with self.assertRaisesRegex(ValueError, 'descriptor fields'):
            subject.descriptor({'digest': 'sha256:' + 'a' * 64, 'size': 1,
                                'mediaType': subject.MANIFEST, 'urls': ['https://example.com']})
        with self.assertRaisesRegex(ValueError, 'duplicate JSON'):
            subject.parse(b'{"auth":1,"auth":2}')

    def test_public_host_allowlist(self):
        for url in ('http://registry-1.docker.io/x', 'https://secret@registry-1.docker.io/x',
                    'https://registry-1.docker.io:8443/x', 'https://evil.example/x',
                    'https://r2.cloudflarestorage.com.evil.example/x',
                    'https://registry-1.docker.io/x#fragment'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                subject._destination(url, blob=True)
        subject._destination('https://production.cloudflare.docker.com/x', blob=True)
        with self.assertRaises(ValueError):
            subject._destination('https://production.cloudflare.docker.com/x', blob=False)

    def test_wrapper_has_only_selected_platform(self):
        source = FakePublicRegistry()
        selected = subject.parse(source.index)['manifests'][0]
        wrapper = subject.parse(subject.selected_wrapper(selected))
        self.assertEqual(len(wrapper['manifests']), 1)
        self.assertEqual(wrapper['manifests'][0]['digest'], selected['digest'])
        self.assertEqual(wrapper['manifests'][0]['annotations']['io.containerd.image.name'],
                         'docker.io/library/registry:3.1.1')

    def test_cdn_redirect_never_receives_registry_authorization(self):
        class Response(io.BytesIO):
            status = 200
            headers = {}
        client = subject.PublicRegistry()
        client._token = 'private-anonymous-pull-token'
        calls = []
        def opened(request, **kwargs):
            calls.append(request)
            if len(calls) == 1:
                raise urllib.error.HTTPError(request.full_url, 307, 'redirect',
                    {'Location': 'https://production.cloudfront.docker.com/publicblob'}, io.BytesIO())
            return Response(b'public blob')
        with mock.patch.object(client.opener, 'open', side_effect=opened):
            raw, receipt = client.get(subject.REGISTRY + 'blobs/sha256:' + 'a' * 64,
                                      limit=64, authenticated=True, blob=True)
        self.assertEqual(raw, b'public blob')
        self.assertEqual(calls[0].get_header('Authorization'), 'Bearer private-anonymous-pull-token')
        self.assertIsNone(calls[1].get_header('Authorization'))
        self.assertNotIn('token', repr(receipt))

    def test_streaming_deadline_rejects_without_waiting_for_inactivity(self):
        class Response(io.BytesIO):
            status = 200
            headers = {}
        client = subject.PublicRegistry()
        with mock.patch.object(client.opener, 'open', return_value=Response(b'public blob')), \
                mock.patch.object(subject.time, 'monotonic', side_effect=[0, 0, 61]):
            with self.assertRaisesRegex(ValueError, 'public request deadline'):
                client.get(subject.REGISTRY + 'blobs/sha256:' + 'a' * 64, limit=64)

    def test_invalid_frozen_pin_rejected_before_creation_or_network(self):
        transport = FakePublicRegistry()
        with self.assertRaises(ValueError):
            subject.acquire(self.root / 'bad', pins={'schema_version': True}, transport=transport)
        self.assertEqual(transport.calls, [])
        self.assertFalse((self.root / 'bad').exists())


if __name__ == '__main__':
    unittest.main()
