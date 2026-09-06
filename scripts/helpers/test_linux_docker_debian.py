"""Adversarial parser tests; no downloads, package installs, or guest execution."""

import hashlib
import io
import lzma
import tarfile
import unittest

import linux_docker_debian as debian


def compressed_control(raw=b"Package: openssh-client\nVersion: 1:9.2p1-2+deb12u10\nArchitecture: arm64\n", *, name="./control", kind=tarfile.REGTYPE):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        member = tarfile.TarInfo(name)
        member.type = kind
        member.size = len(raw) if kind == tarfile.REGTYPE else 0
        member.linkname = "/outside" if kind == tarfile.SYMTYPE else ""
        archive.addfile(member, io.BytesIO(raw) if member.size else None)
    return lzma.compress(output.getvalue())


def ar_member(name, data):
    return (name.encode().ljust(16) + b"0".ljust(12) + b"0".ljust(6) + b"0".ljust(6)
            + b"100644".ljust(8) + str(len(data)).encode().ljust(10) + b"`\n"
            + data + (b"\n" if len(data) % 2 else b""))


def deb(control=None):
    return (b"!<arch>\n" + ar_member("debian-binary", b"2.0\n")
            + ar_member("control.tar.xz", compressed_control() if control is None else control)
            + ar_member("data.tar.xz", lzma.compress(b"opaque payload")))


class DebianMetadataTests(unittest.TestCase):
    def test_paragraphs_preserve_continuations(self):
        raw = b"Package: first\nDescription: title\n body\n .\n\nPackage: second\n"
        self.assertEqual(list(debian.paragraphs(raw)), [
            {"Package": "first", "Description": "title\nbody\n."}, {"Package": "second"}])

    def test_ambiguous_metadata_rejected(self):
        for raw in (b"", b"Package: a", b"Package: a\r\n", b"Package: a\x00\n",
                    b" continuation\n", b"Package: a\npackage: b\n", b"Invalid field: a\n",
                    b"Package: \xff\n", b"broken\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                list(debian.paragraphs(raw))

    def test_bounds_and_single_paragraph(self):
        with self.assertRaises(ValueError):
            list(debian.paragraphs(b"Key: value\n", limit=4))
        with self.assertRaises(ValueError):
            debian.one_paragraph(b"Key: first\n\nKey: second\n")
        with self.assertRaises(ValueError):
            list(debian.paragraphs(b"Key: " + b"a" * (256 * 1024) + b"\n"))

    def test_repository_paths(self):
        self.assertEqual(debian.relative_path("pool/main/o/openssh/openssh_9.2+deb12_arm64.deb"),
                         "pool/main/o/openssh/openssh_9.2+deb12_arm64.deb")
        for value in ("/absolute", "../bad", "a/../b", "a//b", "./a", "a/", "a b", "a%2fb", "a\\b", ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                debian.relative_path(value)

    def test_release_selects_exact_authenticated_descriptor(self):
        raw = ("Origin: Debian\nSHA256:\n " + "a" * 64 + " 123 main/binary-arm64/Packages.xz\n "
               + "b" * 64 + " 0 empty\n").encode()
        self.assertEqual(debian.release_entry(raw, "main/binary-arm64/Packages.xz"),
                         {"sha256": "a" * 64, "size": 123})
        for changed, path in ((raw, "missing"), (raw, "empty"),
                              (raw + (" " + "c" * 64 + " 1 empty\n").encode(), "empty"),
                              (raw.replace(b"123 ", b"0123 "), "main/binary-arm64/Packages.xz"),
                              (raw.replace(b"123 ", b"True "), "main/binary-arm64/Packages.xz")):
            with self.subTest(path=path, changed=changed), self.assertRaises(ValueError):
                debian.release_entry(changed, path)

    @staticmethod
    def package():
        return ("Package: openssh-client\nVersion: 1:9.2p1-2+deb12u10\nArchitecture: arm64\n"
                "Filename: pool/main/o/openssh/openssh-client_9.2_arm64.deb\nSize: 123\nSHA256: "
                + "a" * 64 + "\n").encode()

    def test_package_exact_identity(self):
        result = debian.package_entry(self.package(), name="openssh-client", version="1:9.2p1-2+deb12u10")
        self.assertEqual((result["size"], result["sha256"]), (123, "a" * 64))
        for raw in (self.package().replace(b"arm64\n", b"amd64\n"),
                    self.package().replace(b"deb12u10", b"deb12u11"),
                    self.package() + b"\n" + self.package(),
                    self.package().replace(b"pool/main", b"../main"),
                    self.package().replace(b"Size: 123", b"Size: -1")):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                debian.package_entry(raw, name="openssh-client", version="1:9.2p1-2+deb12u10")

    def test_verify_content(self):
        raw = b"frozen input"
        expected = {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        self.assertEqual(debian.verify_bytes(raw, expected), raw)
        for changed in ({**expected, "size": True}, {**expected, "sha256": "b" * 64},
                        {**expected, "size": len(raw) + 1}, {**expected, "url": "alternate"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                debian.verify_bytes(raw, changed)

    def test_bounded_xz(self):
        raw = b"test" * 100
        compressed = lzma.compress(raw)
        self.assertEqual(debian.unxz(compressed, limit=len(raw)), raw)
        for changed, limit in ((compressed, len(raw) - 1), (compressed[:-1], len(raw)),
                               (compressed + b"extra", len(raw)), (compressed * 2, len(raw) * 2),
                               (b"not xz", len(raw))):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                debian.unxz(changed, limit=limit)

    def test_signature_status_binds_primary_fingerprints(self):
        primary = "B8B80B5B623EAB6AD8775C45B7C5D7D6350947F8"
        signing = "4CB50190207B4758A3F73A796ED0E7B82643E131"
        raw = ("gpgv: Signature made\n[GNUPG:] NEWSIG\n"
               "[GNUPG:] GOODSIG 6ED0E7B82643E131 Debian Archive\n"
               f"[GNUPG:] VALIDSIG {signing} 2026-07-11 1783765031 0 4 0 1 8 01 {primary}\n").encode()
        def verify(value):
            return debian.signature_status(value, required=[primary], allowed=[primary], signed_before=1788220800)
        self.assertEqual(verify(raw)[0]["primary_fingerprint"], primary)
        self.assertEqual(verify(b"[GNUPG:] PLAINTEXT 74 0 \n" + raw), verify(raw))
        for changed in (raw.replace(b"GOODSIG", b"EXPKEYSIG"), raw.replace(b" 1 8 01 ", b" 1 2 01 "),
                        raw.replace(primary.encode(), b"A" * 40), raw.replace(b"2026-07-11", b"2026-07-12"),
                        raw.replace(b"1783765031", b"1883765031"), raw + raw,
                        raw + b"[GNUPG:] BADSIG 0000000000000000 bad\n",
                        raw.replace(b"[GNUPG:] NEWSIG\n", b""),
                        raw.split(b"[GNUPG:] VALIDSIG")[0],
                        b"[GNUPG:] PLAINTEXT 62 0 \n" + raw,
                        b"[GNUPG:] PLAINTEXT 74 0 named-file\n" + raw,
                        raw + b"[GNUPG:] PLAINTEXT 74 0 \n"):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                verify(changed)

    def test_deb_control_exact_variant(self):
        self.assertEqual(debian.deb_control(deb()), {"Package": "openssh-client",
                         "Version": "1:9.2p1-2+deb12u10", "Architecture": "arm64"})

    def test_deb_container_errors(self):
        raw = deb()
        for changed in (raw[:-1], raw + b"x", raw.replace(b"2.0\n", b"3.0\n", 1),
                        raw.replace(b"control.tar.xz", b"control.tar.gz", 1),
                        raw.replace(b"!<arch>\n", b"invalid\n", 1),
                        raw.replace(b"data.tar.xz", b"evil.tar.xz", 1)):
            with self.subTest(changed=changed[:100]), self.assertRaises(ValueError):
                debian.deb_control(changed)

    def test_control_paths_types_and_bounds(self):
        for compressed in (compressed_control(name="../control"), compressed_control(name="/control"),
                           compressed_control(name="./nested/control"), compressed_control(kind=tarfile.SYMTYPE),
                           compressed_control(raw=b"x" * debian.MAX_CONTROL),
                           compressed_control(raw=b"Package: a\nPackage: b\n")):
            with self.subTest(size=len(compressed)), self.assertRaises(ValueError):
                debian.deb_control(deb(compressed))

    def test_control_tar_trailing_data_rejected(self):
        raw = lzma.decompress(compressed_control())
        for changed in (raw + b"hidden", raw + raw, raw[:1024], raw[:-1]):
            with self.subTest(size=len(changed)), self.assertRaises(ValueError):
                debian.deb_control(deb(lzma.compress(changed)))


if __name__ == "__main__":
    unittest.main()
