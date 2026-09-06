"""Bounded offline Debian metadata parsing; parsing is not authentication.

Callers must authenticate Release bytes with the admitted archive keyring, then
bind Packages and DEB bytes through their signed SHA256 descriptors. Nothing in
this module installs packages, executes maintainer scripts, or extracts paths.
"""

import hashlib
import io
import lzma
from datetime import datetime, timezone
from pathlib import PurePosixPath
import re
import tarfile

from linux_docker_image_input import require

MAX_METADATA = 64 * 1024 * 1024
MAX_DEB = 32 * 1024 * 1024
MAX_CONTROL = 1024 * 1024
SHA256 = re.compile(r"[0-9a-f]{64}")


def paragraphs(raw, *, limit=MAX_METADATA):
    """Yield unambiguous Deb822 fields, retaining continuation newlines.

    Field names are case-insensitive in Deb822; canonical spelling is preserved
    while case-only duplicates are rejected. Limits apply before text decoding.
    """
    require(isinstance(raw, bytes) and 0 < len(raw) <= limit, "Deb822 input size")
    require(b"\r" not in raw and b"\x00" not in raw and raw.endswith(b"\n"), "Deb822 framing")
    text = raw.decode("utf-8", errors="strict")
    current, names, previous = {}, set(), None
    count = 0
    for line in text.split("\n"):
        require(len(line) <= 256 * 1024, "Deb822 line bound")
        if not line:
            if current:
                count += 1
                require(count <= 100_000, "Deb822 paragraph bound")
                yield current
                current, names, previous = {}, set(), None
            continue
        if line[0] in " \t":
            require(previous is not None, "orphan Deb822 continuation")
            current[previous] += "\n" + line[1:]
            require(len(current[previous]) <= 2 * 1024 * 1024, "Deb822 field bound")
            continue
        name, separator, value = line.partition(":")
        require(separator and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", name), "Deb822 field name")
        require(name.lower() not in names, "duplicate Deb822 field")
        require(len(current) < 128, "Deb822 field count")
        names.add(name.lower())
        current[name] = value.lstrip(" \t")
        previous = name


def one_paragraph(raw, *, limit=MAX_CONTROL):
    rows = list(paragraphs(raw, limit=limit))
    require(len(rows) == 1, "exactly one Deb822 paragraph required")
    return rows[0]


def relative_path(value):
    require(isinstance(value, str) and 0 < len(value) <= 1024
            and re.fullmatch(r"[A-Za-z0-9._+/-]+", value), "repository path grammar")
    path = PurePosixPath(value)
    require(not path.is_absolute() and str(path) == value
            and all(part not in {".", ".."} for part in path.parts), "repository path traversal")
    return value


def descriptor(sha256, size):
    require(isinstance(sha256, str) and SHA256.fullmatch(sha256), "SHA256 descriptor")
    require(isinstance(size, str) and re.fullmatch(r"[1-9][0-9]{0,10}", size), "size descriptor")
    count = int(size)
    require(count <= MAX_METADATA, "descriptor size bound")
    return {"sha256": sha256, "size": count}


def verify_bytes(raw, expected):
    require(isinstance(raw, bytes) and isinstance(expected, dict)
            and set(expected) == {"sha256", "size"}, "byte descriptor fields")
    require(type(expected["size"]) is int and 0 < expected["size"] <= MAX_METADATA
            and isinstance(expected["sha256"], str) and SHA256.fullmatch(expected["sha256"]),
            "byte descriptor values")
    require(len(raw) == expected["size"] and hashlib.sha256(raw).hexdigest() == expected["sha256"],
            "authenticated byte descriptor differs")
    return raw


def signature_status(raw, *, required, allowed, signed_before):
    """Replay GnuPG 2.4 status records from a successful pinned gpgv command.

    gpgv trusts its supplied keyring; this additionally binds the exact primary
    signer set and strong signature algorithms. This is not a live key-revocation
    lookup. The caller must separately require exit zero, no truncation, and the
    admitted explicit keyring and signed input bytes.
    """
    require(isinstance(raw, bytes) and 0 < len(raw) <= 64 * 1024
            and raw.endswith(b"\n") and b"\x00" not in raw, "signature status framing")
    require(type(signed_before) is int and signed_before > 0, "signature timestamp ceiling")
    require(isinstance(required, (tuple, list)) and isinstance(allowed, (tuple, list))
            and 0 < len(required) <= len(allowed) <= 8
            and len(set(required)) == len(required) and len(set(allowed)) == len(allowed)
            and set(required) <= set(allowed)
            and all(isinstance(x, str) and re.fullmatch(r"[0-9A-F]{40}", x) for x in allowed),
            "signature signer policy")
    signatures, current, plaintext_seen = [], None, False
    for line in raw.decode("utf-8", errors="strict").splitlines():
        # stderr may also contain human-readable diagnostics. Machine status
        # records alone determine signature acceptance, never diagnostic prose.
        if not line.startswith("[GNUPG:] "):
            require(not line.startswith("[GNUPG:"), "malformed signature status prefix")
            continue
        fields = line[len("[GNUPG:] "):].split()
        require(fields, "empty signature status")
        code, args = fields[0], fields[1:]
        require(code in {"PLAINTEXT", "NEWSIG", "KEY_CONSIDERED", "SIG_ID", "GOODSIG", "VALIDSIG"},
                "unsupported or unsuccessful signature status")
        if code == "PLAINTEXT":
            require(not plaintext_seen and not signatures and args == ["74", "0"],
                    "unexpected signed plaintext framing")
            plaintext_seen = True
            continue
        if code == "NEWSIG":
            require(not args and (current is None or set(current) == {"good", "valid"}),
                    "incomplete signature group")
            current = {}
            signatures.append(current)
            require(len(signatures) <= 8, "signature group bound")
            continue
        require(current is not None, "signature status before NEWSIG")
        if code == "GOODSIG":
            require("good" not in current and len(args) >= 2
                    and re.fullmatch(r"[0-9A-F]{16}", args[0]), "signature good-key record")
            current["good"] = args[0]
        elif code == "VALIDSIG":
            require("valid" not in current and "good" in current and len(args) == 10,
                    "signature validation group")
            fingerprint, date, stamp, expiry, version, reserved, public, digest, kind, primary = args
            require(re.fullmatch(r"[0-9A-F]{40}", fingerprint)
                    and fingerprint.endswith(current["good"]) and primary in allowed,
                    "signature fingerprint differs")
            require(re.fullmatch(r"[1-9][0-9]{0,10}", stamp) and int(stamp) <= signed_before
                    and datetime.fromtimestamp(int(stamp), timezone.utc).strftime("%Y-%m-%d") == date,
                    "signature creation time differs")
            require((expiry, version, reserved, kind) == ("0", "4", "0", "01")
                    and public in {"1", "22"} and digest in {"8", "10"},
                    "unsupported signature algorithms or expiration")
            current["valid"] = {"primary_fingerprint": primary, "signing_fingerprint": fingerprint,
                                "created_unix": int(stamp), "public_key_algorithm": int(public),
                                "digest_algorithm": int(digest)}
    require(signatures and all(set(item) == {"good", "valid"} for item in signatures),
            "incomplete signature proof")
    primary = [item["valid"]["primary_fingerprint"] for item in signatures]
    require(len(set(primary)) == len(primary) and set(required) <= set(primary), "required signer set differs")
    return [item["valid"] for item in signatures]


def release_entry(raw, path):
    """Find one exact SHA256 path in already authenticated Release plaintext."""
    relative_path(path)
    release = one_paragraph(raw)
    require("SHA256" in release, "Release lacks canonical SHA256 field")
    rows = {}
    for line in release["SHA256"].splitlines():
        if not line:
            continue
        fields = line.split()
        require(len(fields) == 3, "Release SHA256 row")
        checksum, size, name = fields
        relative_path(name)
        require(name not in rows, "duplicate Release path")
        # Releases also list empty metadata files. They are never our selected
        # Packages input, but remain unambiguous authenticated index entries.
        require(SHA256.fullmatch(checksum) and re.fullmatch(r"0|[1-9][0-9]{0,10}", size),
                "Release descriptor grammar")
        rows[name] = (checksum, size)
    require(path in rows, "selected Packages path absent from Release")
    return descriptor(*rows[path])


def package_entry(raw, *, name, version, architecture="arm64"):
    """Select one exact package/version/architecture, never a latest candidate."""
    require(isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9+.-]+", name), "package name")
    require(isinstance(version, str) and re.fullmatch(r"[0-9][A-Za-z0-9.+:~\-]*", version), "package version")
    require(architecture in {"arm64", "all"}, "package architecture")
    matches = []
    for row in paragraphs(raw):
        if (row.get("Package"), row.get("Version"), row.get("Architecture")) == (name, version, architecture):
            matches.append(row)
            require(len(matches) == 1, "duplicate selected package")
    require(len(matches) == 1, "selected package identity absent")
    row = matches[0]
    require({"Filename", "SHA256", "Size"} <= set(row), "package content descriptor absent")
    path = relative_path(row["Filename"])
    require(path.startswith("pool/") and path.endswith(".deb"), "unexpected DEB repository path")
    return {"filename": path, **descriptor(row["SHA256"], row["Size"]), "fields": row}


def unxz(raw, *, limit):
    """Decode exactly one bounded XZ stream; reject appended/concatenated data."""
    require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_METADATA, "XZ input bound")
    require(type(limit) is int and 0 < limit <= MAX_METADATA, "XZ output bound")
    decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=64 * 1024 * 1024)
    try:
        output = decoder.decompress(raw, max_length=limit + 1)
    except lzma.LZMAError as error:
        raise ValueError("invalid or excessive XZ stream") from error
    require(len(output) <= limit and decoder.eof and not decoder.unused_data,
            "XZ truncated, appended, or exceeds bound")
    return output


def deb_control(raw):
    """Inspect the exact three-member XZ DEB variant used by this fixture.

    This only parses the small control archive. The data archive remains opaque
    until the separately authenticated, pinned guest extraction stage.
    """
    require(isinstance(raw, bytes) and 8 < len(raw) <= MAX_DEB and raw[:8] == b"!<arch>\n", "DEB ar header")
    position, members = 8, {}
    expected = ("debian-binary", "control.tar.xz", "data.tar.xz")
    for name in expected:
        header = raw[position:position + 60]
        require(len(header) == 60 and header[58:] == b"`\n", "DEB member header")
        require(header[:16].rstrip(b" ").decode("ascii") == name, "DEB member name/order")
        size_text = header[48:58].rstrip(b" ")
        require(re.fullmatch(rb"0|[1-9][0-9]*", size_text), "DEB member size")
        size = int(size_text)
        require(0 < size <= MAX_DEB, "DEB member bound")
        position += 60
        require(position + size <= len(raw), "truncated DEB member")
        members[name] = raw[position:position + size]
        position += size
        if size % 2:
            require(raw[position:position + 1] == b"\n", "DEB ar padding")
            position += 1
    require(position == len(raw) and members["debian-binary"] == b"2.0\n", "DEB version or appended member")
    control_tar = unxz(members["control.tar.xz"], limit=MAX_CONTROL)
    control, seen = None, set()
    try:
        with tarfile.open(fileobj=io.BytesIO(control_tar), mode="r:") as archive:
            for item in archive:
                require(len(seen) < 128 and item.name not in seen, "control archive inventory")
                seen.add(item.name)
                require(item.name == "." or (item.name.startswith("./")
                        and "/" not in item.name[2:] and item.name[2:] not in {"", ".", ".."}),
                        "control archive path")
                require(item.isdir() if item.name == "." else item.isfile(), "control archive member type")
                require(not item.pax_headers and 0 <= item.size <= MAX_CONTROL, "control archive metadata bound")
                if item.name == "./control":
                    control = archive.extractfile(item).read(MAX_CONTROL + 1)
            trailer = control_tar[archive.offset:]
            require(len(control_tar) % 512 == 0 and len(trailer) >= 1024
                    and not any(trailer), "control TAR terminator or trailing bytes")
    except tarfile.TarError as error:
        raise ValueError("invalid DEB control TAR") from error
    require(control is not None, "DEB control file absent")
    return one_paragraph(control)
