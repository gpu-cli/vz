"""Inert transport adversaries; no network is used and nothing is verified here.

Acquisition is not verification: `linux_docker_ssh_input.verify` remains the
only thing that decides these bytes are trustworthy. What these cover is that a
byte which is not exactly what the pin names never reaches disk, and that a row
the pin cannot locate is never invented.
"""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import linux_docker_ssh_acquire as subject


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


BODIES = {
    'dists/bookworm/InRelease': b'release bytes\n',
    'dists/bookworm/main/binary-arm64/Packages.xz': b'packages index\n',
    'pool/main/liba/libaaa/libaaa_1_arm64.deb': b'a deb\n',
    'dists/bookworm/main/source/Sources.xz': b'sources\n',
}


def pin():
    def row(path, name):
        return {'filename': name, 'repository_path': path,
                'sha256': digest(BODIES[path]), 'size': len(BODIES[path])}
    return {
        'snapshot': {'base_url': 'https://snapshot.debian.org/archive/debian/20260901T000000Z/',
                     'suite': 'bookworm'},
        'bounds': {'release_bytes': 1024, 'packages_compressed_bytes': 1024, 'deb_bytes_each': 1024},
        'release': row('dists/bookworm/InRelease', 'bookworm.InRelease'),
        'packages_index': {'filename': 'bookworm-Packages.xz',
                           'release_path': 'main/binary-arm64/Packages.xz',
                           'sha256': digest(BODIES['dists/bookworm/main/binary-arm64/Packages.xz']),
                           'size': len(BODIES['dists/bookworm/main/binary-arm64/Packages.xz'])},
        'packages': [row('pool/main/liba/libaaa/libaaa_1_arm64.deb', 'libaaa_1_arm64.deb')],
        'source_proofs': [row('dists/bookworm/main/source/Sources.xz', 'bookworm-Sources.xz'),
                          {'filename': 'base-evidence.json', 'sha256': 'e' * 64, 'size': 3}],
    }


class FakeTransport:
    def __init__(self, bodies=None):
        self.bodies = dict(BODIES if bodies is None else bodies)
        self.requested = []

    def get(self, repository_path, *, limit):
        self.requested.append((repository_path, limit))
        raw = self.bodies.get(repository_path)
        if raw is None:
            raise ValueError('ssh input acquisition: HTTP request rejected')
        return raw


class AcquireTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/private/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_every_locatable_row_is_fetched_and_provenance_is_left_alone(self):
        transport = FakeTransport()
        written = subject.acquire(self.root / 'inputs', pin(), transport=transport)
        self.assertEqual(sorted(written),
                         ['bookworm-Packages.xz', 'bookworm-Sources.xz', 'bookworm.InRelease',
                          'libaaa_1_arm64.deb'])
        # The row the pin cannot locate is never requested and never invented.
        self.assertNotIn('base-evidence.json', written)
        self.assertFalse((self.root / 'inputs' / 'base-evidence.json').exists())
        for path, limit in transport.requested:
            self.assertIn(path, BODIES)
            self.assertLessEqual(len(BODIES[path]), limit)

    def test_the_packages_index_is_located_under_its_suite(self):
        transport = FakeTransport()
        subject.acquire(self.root / 'inputs', pin(), transport=transport)
        self.assertIn(('dists/bookworm/main/binary-arm64/Packages.xz', 1024), transport.requested)

    def test_a_wrong_digest_never_reaches_disk(self):
        bodies = dict(BODIES)
        bodies['pool/main/liba/libaaa/libaaa_1_arm64.deb'] = b'a deb\n'.replace(b'a', b'b')
        with self.assertRaisesRegex(ValueError, 'wrong (digest|size)'):
            subject.acquire(self.root / 'inputs', pin(), transport=FakeTransport(bodies))
        self.assertFalse((self.root / 'inputs' / 'libaaa_1_arm64.deb').exists())

    def test_a_wrong_size_never_reaches_disk(self):
        bodies = dict(BODIES)
        bodies['dists/bookworm/InRelease'] = b'release bytes\nextra\n'
        with self.assertRaisesRegex(ValueError, 'wrong size'):
            subject.acquire(self.root / 'inputs', pin(), transport=FakeTransport(bodies))
        self.assertFalse((self.root / 'inputs' / 'bookworm.InRelease').exists())

    def test_the_destination_must_be_fresh(self):
        (self.root / 'existing').mkdir()
        with self.assertRaisesRegex(ValueError, 'fresh canonical destination'):
            subject.acquire(self.root / 'existing', pin(), transport=FakeTransport())
        with self.assertRaisesRegex(ValueError, 'fresh canonical destination'):
            subject.acquire(Path('relative/path'), pin(), transport=FakeTransport())

    def test_a_filename_that_is_not_local_is_refused(self):
        bad = pin()
        bad['packages'][0]['filename'] = '../escape.deb'
        with self.assertRaisesRegex(ValueError, 'filename must be local'):
            subject.acquire(self.root / 'inputs', bad, transport=FakeTransport())


class TransportTests(unittest.TestCase):
    def transport(self):
        return subject.PublicSnapshot('https://snapshot.debian.org/archive/debian/20260901T000000Z/')

    def test_only_https_and_a_real_host_are_admitted(self):
        for bad in ('http://snapshot.debian.org/', 'file:///etc/', 'https:///nohost'):
            with self.assertRaisesRegex(ValueError, 'snapshot base URL'):
                subject.PublicSnapshot(bad)

    def test_a_path_may_not_escape_the_snapshot(self):
        transport = self.transport()
        for bad in ('/etc/passwd', '../../etc/passwd', 'dists/../../escape', ''):
            with self.assertRaisesRegex(ValueError, 'repository path'):
                transport.get(bad, limit=1024)

    def test_a_response_bound_is_required(self):
        transport = self.transport()
        for bad in (0, -1, 1024 * 1024 * 1024, 'big'):
            with self.assertRaisesRegex(ValueError, 'response bound'):
                transport.get('dists/bookworm/InRelease', limit=bad)
