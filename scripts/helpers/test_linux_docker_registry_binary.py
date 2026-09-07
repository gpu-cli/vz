"""Synthetic parser controls, never evidence of an actual registry execution."""
import hashlib
import io
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_registry_binary as subject


def encoded(raw):
    size, prefix = len(raw), bytearray()
    while size > 127:
        prefix.append((size & 127) | 128)
        size >>= 7
    return bytes(prefix) + bytes([size]) + raw


def binary(*, version=subject.GO_VERSION, extra='', build=None, frame=True):
    build = subject.BUILD if build is None else build
    module = ('path\t' + subject.MAIN_PATH + '\nmod\t' + subject.MODULE_PATH + '\t' +
              subject.MODULE_VERSION + '\t\n' + ''.join('build\t' + k + '=' + v + '\n'
              for k, v in build.items()) + extra).encode()
    framed = subject.START + module + subject.END if frame else module
    header = bytearray(64)
    header[:6] = b'\x7fELF\x02\x01'
    struct.pack_into('<H', header, 18, 183)
    return bytes(header) + subject.MAGIC + b'\x08\x02' + bytes(16) + encoded(version.encode()) + encoded(framed)


class BinaryTests(unittest.TestCase):
    def admitted(self, raw):
        with patch.object(subject, 'BINARY_SIZE', len(raw)), patch.object(
                subject, 'BINARY_SHA256', hashlib.sha256(raw).hexdigest()):
            return subject.validate_binary(raw)

    def test_exact_compiled_metadata_and_scope(self):
        proof = self.admitted(binary())
        self.assertEqual(proof['go_version'], 'go1.25.9')
        self.assertEqual(proof['module_version'], 'v3.1.1')
        self.assertEqual(proof['build_settings']['vcs.revision'], subject.VCS_REVISION)
        self.assertFalse(proof['binary_executed'])
        self.assertFalse(proof['reproducible_build_certified'])

    def test_real_pin_rejects_synthetic(self):
        with self.assertRaises(subject.BinaryError):
            subject.validate_binary(binary())

    def test_changed_go_and_settings_even_with_resealed_fixture_hash(self):
        variants = [binary(version='go1.25.8'), binary(build=dict(subject.BUILD, GOARCH='amd64')),
                    binary(build=dict(subject.BUILD, unexpected='value'))]
        for raw in variants:
            with self.assertRaises(subject.BinaryError):
                self.admitted(raw)

    def test_duplicate_setting_and_module_rejected(self):
        for extra in ('build\tGOOS=linux\n', 'mod\tforeign\tv0\t\n',
                      'path\tforeign\n', 'unknown\tvalue\n'):
            with self.assertRaises(subject.BinaryError):
                self.admitted(binary(extra=extra))

    def test_dependency_records_permitted_under_whole_binary_pin(self):
        self.assertEqual(self.admitted(binary(extra='dep\texample.invalid/module\tv1.0.0\th1:pin\n'))[
            'main_path'], subject.MAIN_PATH)

    def test_wrong_elf_arch_flags_and_header_rejected(self):
        original = binary()
        for offset, value in ((0, 0), (4, 1), (5, 2), (18, 62), (78, 4), (79, 0), (80, 1)):
            raw = bytearray(original); raw[offset] = value
            with self.assertRaises(subject.BinaryError):
                self.admitted(bytes(raw))

    def test_duplicate_aligned_header_rejected(self):
        raw = binary()
        raw += bytes(-len(raw) % 16) + subject.MAGIC + bytes(18)
        with self.assertRaises(subject.BinaryError):
            self.admitted(raw)

    def test_framing_and_truncation_rejected(self):
        for raw in (binary(frame=False), binary()[:-1], binary()[:96], binary()[:96] + b'\x80' * 10):
            with self.assertRaises(subject.BinaryError):
                self.admitted(raw)

    def test_varint_bounds_and_canonical_encoding(self):
        for raw in (b'\x80\x00', b'\x80' * 10, b'\x04abc', b'\x00'):
            with self.assertRaises(subject.BinaryError):
                subject._string(raw, 0)

    def test_exact_layer_member_no_extraction_and_after_read_guard(self):
        raw = binary()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            layer = root / 'blobs' / 'sha256' / subject.LAYER_DIGEST[7:]
            layer.parent.mkdir(parents=True)
            with tarfile.open(layer, 'w:gz') as archive:
                member = tarfile.TarInfo('bin/registry'); member.size = len(raw)
                archive.addfile(member, io.BytesIO(raw))
            proof = {'inventory': {'inventory_sha256': 'a' * 64}, 'pins_sha256': 'b' * 64}
            pins = {'layers': [{'digest': subject.LAYER_DIGEST}]}
            with patch.object(subject.fixture, 'validate_layout', return_value=proof) as admitted, \
                    patch.object(subject, 'BINARY_SIZE', len(raw)), \
                    patch.object(subject, 'BINARY_SHA256', hashlib.sha256(raw).hexdigest()), \
                    patch.object(tarfile.TarFile, 'extract', side_effect=AssertionError('extraction')), \
                    patch.object(tarfile.TarFile, 'extractall', side_effect=AssertionError('extraction')):
                self.assertEqual(subject.validate_layout_binary(root, pins=pins)['member'], 'bin/registry')
                self.assertEqual(admitted.call_count, 2)
            self.assertFalse((root / 'bin').exists())

    def test_public_fixed_diagnostic_does_not_echo_binary(self):
        with self.assertRaises(subject.BinaryError) as caught:
            self.admitted(binary(extra='PRIVATE-CONTENT\tvalue\n'))
        self.assertNotIn('PRIVATE-CONTENT', str(caught.exception))

    def test_layer_whiteout_duplicate_and_link_rejected(self):
        raw = binary()
        for mutation in ('whiteout', 'duplicate', 'symlink', 'directory-link'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                layer = root / 'blobs' / 'sha256' / subject.LAYER_DIGEST[7:]
                layer.parent.mkdir(parents=True)
                with tarfile.open(layer, 'w:gz') as archive:
                    member = tarfile.TarInfo('bin/registry'); member.size = len(raw)
                    archive.addfile(member, io.BytesIO(raw))
                    if mutation == 'whiteout':
                        archive.addfile(tarfile.TarInfo('bin/.wh.registry'))
                    elif mutation == 'duplicate':
                        archive.addfile(member, io.BytesIO(raw))
                    else:
                        member = tarfile.TarInfo('bin' if mutation == 'directory-link' else 'bin/registry')
                        member.type = tarfile.SYMTYPE; member.linkname = '/foreign'
                        archive.addfile(member)
                proof = {'inventory': {'inventory_sha256': 'a' * 64}, 'pins_sha256': 'b' * 64}
                with patch.object(subject.fixture, 'validate_layout', return_value=proof), \
                        patch.object(subject, 'BINARY_SIZE', len(raw)), \
                        patch.object(subject, 'BINARY_SHA256', hashlib.sha256(raw).hexdigest()):
                    with self.assertRaises(subject.BinaryError):
                        subject.validate_layout_binary(root, pins={'layers': [{'digest': subject.LAYER_DIGEST}]})


if __name__ == '__main__':
    unittest.main()
