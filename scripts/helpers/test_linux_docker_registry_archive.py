"""Synthetic deterministic load-input tests; never dispatch Docker or a VM."""
import io
import json
import os
from pathlib import Path
import runpy
import tarfile
import tempfile
import unittest
from unittest import mock

import linux_docker_registry_archive as archive
import linux_docker_registry_fixture as fixture
from test_linux_docker_registry_fixture import synthetic


class RegistryArchiveTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='vz-registry-archive-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.layout = self.root / 'layout'
        self.pins, self.files = synthetic()
        (self.layout / 'blobs/sha256').mkdir(parents=True)
        for name, raw in self.files.items():
            (self.layout / name).write_bytes(raw)

    def create(self, name='registry.tar'):
        path = self.root / name
        return path, archive.create_archive(self.layout, pins=self.pins, output=path)

    def test_repeated_creation_is_exact_and_headers_are_canonical(self):
        first, proof = self.create()
        second, second_proof = self.create('second.tar')
        self.assertEqual(first.read_bytes(), second.read_bytes())
        self.assertEqual(proof, second_proof)
        self.assertEqual(proof['archive_sha256'], fixture.sha(first.read_bytes()))
        self.assertEqual(proof['archive_bytes'], len(first.read_bytes()))
        self.assertFalse(proof['docker_load_certified'])
        self.assertFalse(proof['registry_execution_certified'])
        with tarfile.open(fileobj=io.BytesIO(first.read_bytes()), mode='r:') as reader:
            self.assertEqual(reader.getnames(), sorted(self.files))
            for item in reader:
                self.assertTrue(item.isfile())
                self.assertEqual((item.mode, item.uid, item.gid, item.mtime), (0o644, 0, 0, 0))
                self.assertEqual((item.uname, item.gname, item.linkname), ('', '', ''))
                self.assertEqual(reader.extractfile(item).read(), self.files[item.name])
        self.assertTrue(first.read_bytes().endswith(bytes(1024)))
        self.assertEqual(first.stat().st_mode & 0o777, 0o600)

    def test_verification_has_no_write_dispatch_or_network(self):
        path, proof = self.create()
        original = os.open

        def readonly(name, flags, *args, **kwargs):
            self.assertFalse(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            return original(name, flags, *args, **kwargs)

        with mock.patch('subprocess.Popen', side_effect=AssertionError('dispatch')), \
             mock.patch('socket.socket', side_effect=AssertionError('network')), \
             mock.patch('os.open', side_effect=readonly):
            self.assertEqual(archive.validate_archive(path, layout=self.layout, pins=self.pins), proof)

    def test_existing_file_and_symlink_are_never_overwritten(self):
        output = self.root / 'existing'
        output.write_bytes(b'foreign-preserved')
        link = self.root / 'alias'
        link.symlink_to(output)
        for selected in (output, link):
            with self.assertRaises(archive.ArchiveError):
                archive.create_archive(self.layout, pins=self.pins, output=selected)
            self.assertEqual(output.read_bytes(), b'foreign-preserved')
        self.assertTrue(link.is_symlink())

    def test_output_inside_layout_is_rejected_before_mutation(self):
        output = self.layout / 'registry.tar'
        with self.assertRaises(archive.ArchiveError):
            archive.create_archive(self.layout, pins=self.pins, output=output)
        self.assertFalse(output.exists())
        fixture.validate_layout(self.layout, pins=self.pins)

    def test_corruption_truncation_extra_padding_and_reordering_are_rejected(self):
        path, _ = self.create()
        original = path.read_bytes()
        for raw in (b'x' + original[1:], original[:-1], original + bytes(512),
                    original[512:] + original[:512]):
            path.write_bytes(raw)
            with self.assertRaises(archive.ArchiveError):
                archive.validate_archive(path, layout=self.layout, pins=self.pins)

    def test_wrong_external_pins_and_source_change_reject_before_output_creation(self):
        pins = json.loads(json.dumps(self.pins))
        pins['layers'][0]['diff_id'] = 'sha256:' + 'd' * 64
        output = self.root / 'refused.tar'
        with self.assertRaises(archive.ArchiveError):
            archive.create_archive(self.layout, pins=pins, output=output)
        self.assertFalse(output.exists())
        (self.layout / 'index.json').write_bytes(b'{}')
        with self.assertRaises(archive.ArchiveError):
            archive.create_archive(self.layout, pins=self.pins, output=output)
        self.assertFalse(output.exists())

    def test_failure_retains_partial_output_without_returning_proof(self):
        original = archive._chunks

        def interrupted(*args):
            yield next(original(*args))
            raise archive.ArchiveError('synthetic interruption')

        output = self.root / 'partial.tar'
        with mock.patch.object(archive, '_chunks', side_effect=interrupted), self.assertRaises(archive.ArchiveError):
            archive.create_archive(self.layout, pins=self.pins, output=output)
        self.assertTrue(output.exists())
        self.assertEqual(output.stat().st_size, 512)
        with self.assertRaises(archive.ArchiveError):
            archive.validate_archive(output, layout=self.layout, pins=self.pins)

    def test_source_tampering_after_first_admission_is_rejected(self):
        original = archive._chunks

        def changed(layout, admission):
            path = layout / fixture.blob_path(self.pins['layers'][0]['digest'])
            path.write_bytes(path.read_bytes() + b'changed')
            yield from original(layout, admission)

        with mock.patch.object(archive, '_chunks', side_effect=changed), self.assertRaises(archive.ArchiveError):
            self.create()

    def test_archive_symlink_hardlink_and_noncanonical_parent_are_rejected(self):
        path, _ = self.create()
        symlink = self.root / 'linked.tar'
        symlink.symlink_to(path)
        with self.assertRaises(archive.ArchiveError):
            archive.validate_archive(symlink, layout=self.layout, pins=self.pins)
        hardlink = self.root / 'hardlinked.tar'
        os.link(path, hardlink)
        with self.assertRaises(archive.ArchiveError):
            archive.validate_archive(hardlink, layout=self.layout, pins=self.pins)
        directory_alias = self.root / 'directory-alias'
        directory_alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(archive.ArchiveError):
            archive.create_archive(self.layout, pins=self.pins, output=directory_alias / 'refused.tar')
        self.assertFalse((self.root / 'refused.tar').exists())

    def test_fixture_cli_is_offline_and_prints_only_admission_proof(self):
        pin = self.root / 'pin.json'
        pin.write_bytes(fixture.canonical(self.pins))
        output = io.StringIO()
        with mock.patch('sys.argv', ['fixture', '--layout', str(self.layout), '--pin', str(pin)]), \
             mock.patch('sys.stdout', output), mock.patch('subprocess.Popen', side_effect=AssertionError('dispatch')), \
             mock.patch('socket.socket', side_effect=AssertionError('network')):
            runpy.run_module('linux_docker_registry_fixture', run_name='__main__')
        proof = json.loads(output.getvalue())
        self.assertFalse(proof['registry_execution_certified'])
        self.assertEqual(proof['manifest_digest'], self.pins['source']['platform_manifest']['digest'])

    def test_cli_never_resolves_symlinked_pin_or_layout_into_admitted_input(self):
        pin = self.root / 'pin.json'
        pin.write_bytes(fixture.canonical(self.pins))
        pin_alias, layout_alias = self.root / 'pin-alias', self.root / 'layout-alias'
        pin_alias.symlink_to(pin)
        layout_alias.symlink_to(self.layout, target_is_directory=True)
        for module in ('linux_docker_registry_fixture', 'linux_docker_registry_archive'):
            for selected_pin, selected_layout in ((pin_alias, self.layout), (pin, layout_alias)):
                args = [module, '--pin', str(selected_pin), '--layout', str(selected_layout)]
                if module.endswith('_archive'):
                    args += ['--output', str(self.root / 'rejected.tar')]
                with mock.patch('sys.argv', args), mock.patch('sys.stderr', io.StringIO()), \
                     self.assertRaises(SystemExit) as caught:
                    runpy.run_module(module, run_name='__main__')
                self.assertEqual(caught.exception.code, 1)
                self.assertFalse((self.root / 'rejected.tar').exists())


if __name__ == '__main__':
    unittest.main()
