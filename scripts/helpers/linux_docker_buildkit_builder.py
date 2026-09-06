"""Candidate-owned host Buildx builder; never a default daemon or runtime fallback.

The caller authenticates the Machine and owns the installed lifecycle. All Engine
and Buildx operations go through its recorded docker()/mutate() interfaces.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import struct
import tarfile

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_image_input as image_input

REPO = Path(__file__).resolve().parents[2]
CONTRACT = REPO / "config/buildkit-artifact-v0.19.0.json"
LABEL = "dev.vz.buildkit-proof"
LIMIT = 256 * 1024 * 1024
FLAGS = ["--oci-worker=true", "--containerd-worker=false",
         "--oci-worker-binary=/usr/bin/vz-youki", "--oci-worker-snapshotter=overlayfs"]
WRAPPER = b'''#!/bin/busybox sh
set -eu
printf 'vz-youki-invocation pid=%s\n' "$$" >> /var/lib/buildkit/vz-youki-invocations.log
exec /usr/bin/youki "$@"
'''
require = driver.require


def read(path, maximum=LIMIT):
    path = startup.canonical(str(path))
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= maximum,
                "bounded single-link builder input required")
        raw = source.read(maximum + 1)
        after = os.fstat(source.fileno())
        require(len(raw) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "builder input changed while reading")
        return raw


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def static_arm64(raw):
    require(len(raw) >= 64 and raw[:7] == b"\x7fELF\x02\x01\x01", "ELF64 little-endian required")
    require(struct.unpack_from("<HH", raw, 16) == (2, 183), "static AArch64 executable required")
    offset = struct.unpack_from("<Q", raw, 32)[0]
    size, count = struct.unpack_from("<HH", raw, 54)
    require(size == 56 and 0 < count <= 64 and 64 <= offset <= len(raw) - count * size,
            "invalid ELF program table")
    kinds = {struct.unpack_from("<I", raw, offset + index * size)[0] for index in range(count)}
    require(1 in kinds and not kinds.intersection({2, 3}), "dynamic ELF runtime dependency forbidden")


def archive_members(raw, contract):
    require(sha(raw) == contract["archive_sha256"], "BuildKit archive differs from checked-in pin")
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        entries = archive.getmembers()
        require(len(entries) == 3 and {x.name for x in entries} == set(contract["inventory"]),
                "BuildKit archive inventory differs; runtime binaries forbidden")
        result = {}
        for entry in entries:
            require(entry.isfile() and entry.uid == entry.gid == entry.mtime == 0 and
                    0 < entry.size <= LIMIT and entry.mode == (0o644 if entry.name == "manifest.json" else 0o755),
                    "unexpected BuildKit archive entry metadata")
            result[entry.name] = archive.extractfile(entry).read()
        manifest = image_input.parse(result["manifest.json"])
        require(manifest == {"buildkit": contract["buildkit_version"], "layout": contract["layout"],
                             "platform": contract["platform"], "source_commit": contract["source_commit"],
                             "binaries": {name: contract[name + "_sha256"] for name in ("buildctl", "buildkitd")}},
                "BuildKit manifest differs from checked-in source/binary pins")
        for name in ("buildctl", "buildkitd"):
            require(sha(result["bin/" + name]) == contract[name + "_sha256"], "BuildKit binary hash mismatch")
            static_arm64(result["bin/" + name])
        return result


def preflight_archive(value):
    path = startup.canonical(str(value))
    contract_raw = read(CONTRACT, 64 * 1024)
    contract = image_input.parse(contract_raw)
    archive_members(read(path), contract)
    return {"archive": str(path), "sha256": contract["archive_sha256"],
            "contract": str(CONTRACT), "contract_sha256": sha(contract_raw),
            "buildkit_version": contract["buildkit_version"]}


def rootfs_payload(harness):
    pin = harness.info["buildkit"]
    require(preflight_archive(pin["archive"]) == pin, "BuildKit provisioning input changed")
    contract = image_input.parse(read(CONTRACT, 64 * 1024))
    members = archive_members(read(Path(pin["archive"])), contract)
    bundle = harness.prefix / "linux/developer"
    version_path = bundle / "version.json"
    require(startup.digest(version_path) == harness.staged_inputs[str(version_path)], "installed Developer metadata changed")
    version = image_input.parse(read(version_path, 64 * 1024))
    youki = read(bundle / "youki")
    require(sha(youki) == version["sha256_youki"], "installed youki differs from Developer pin")
    static_arm64(youki)
    probe = version["developer_probe"]
    require(probe["archive"] == "developer-probe-rootfs.tar", "unexpected Developer probe archive")
    probe_raw = read(bundle / probe["archive"])
    require(sha(probe_raw) == probe["sha256"], "installed Developer probe changed")
    with tarfile.open(fileobj=io.BytesIO(probe_raw), mode="r:") as archive:
        entries = [x for x in archive.getmembers() if x.name == "bin/busybox"]
        require(len(entries) == 1 and entries[0].isfile() and entries[0].size <= LIMIT,
                "exact regular pinned BusyBox required")
        busybox = archive.extractfile(entries[0]).read()
    require(sha(busybox) == probe["busybox_sha256"], "BusyBox probe binary hash mismatch")
    static_arm64(busybox)
    ca = read(REPO / "linux/ca-trust/cacert.pem", 1024 * 1024)
    ca_pin = image_input.parse(read(REPO / "linux/ca-trust/inputs.json", 64 * 1024))
    require(sha(ca) == ca_pin["bundle_sha256"] == harness.info["public_ca"]["bundle_sha256"],
            "builder CA must match authenticated public guest trust")
    files = {"usr/bin/buildkitd": (members["bin/buildkitd"], 0o755),
             "usr/bin/buildctl": (members["bin/buildctl"], 0o755),
             "usr/bin/youki": (youki, 0o755), "usr/bin/vz-youki": (WRAPPER, 0o755),
             "bin/busybox": (busybox, 0o755), "etc/ssl/certs/ca-certificates.crt": (ca, 0o444),
             "etc/passwd": (b"root:x:0:0:root:/root:/bin/sh\n", 0o444),
             "etc/group": (b"root:x:0:\n", 0o444)}
    # No arbitrary archive extraction, base image, package install or downloaded runtime.
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        directories = {"root", "run", "tmp", "var", "var/lib", "var/lib/buildkit"}
        for name in files:
            directories.update(str(parent) for parent in Path(name).parents if str(parent) != ".")
        for name in sorted(directories):
            entry = tarfile.TarInfo(name)
            entry.type, entry.mode = tarfile.DIRTYPE, 0o1777 if name == "tmp" else 0o755
            archive.addfile(entry)
        for name, (data, mode) in sorted(files.items()):
            entry = tarfile.TarInfo(name)
            entry.size, entry.mode = len(data), mode
            archive.addfile(entry, io.BytesIO(data))
        for applet in ("sh", "mount", "umount", "mkdir", "cat", "ls", "ip", "unshare"):
            entry = tarfile.TarInfo("bin/" + applet)
            entry.type, entry.linkname, entry.mode = tarfile.SYMTYPE, "busybox", 0o777
            archive.addfile(entry)
    inventory = {name: {"sha256": sha(data), "mode": mode, "size": len(data)}
                 for name, (data, mode) in files.items()}
    return output.getvalue(), inventory


def volume_identity(item, name, token):
    require(item["Name"] == name and item["Driver"] == "local" and item["Scope"] == "local" and
            item.get("Labels") == {LABEL: token} and not item.get("Options"), "foreign builder cache volume")
    require(isinstance(item.get("CreatedAt"), str) and item["CreatedAt"] and
            isinstance(item.get("Mountpoint"), str) and item["Mountpoint"].startswith("/"),
            "cache volume identity incomplete")
    return {key: item.get(key) for key in ("Name", "Driver", "Scope", "Labels", "Options", "CreatedAt", "Mountpoint")}


class Builder:
    def __init__(self, harness, descriptor):
        self.harness, self.descriptor = harness, descriptor
        token = sha(json.dumps({"run_id": harness.info["run_id"], "owner": descriptor["owner"]},
                              sort_keys=True).encode())[:24]
        self.token = "vzbuild-" + token
        self.name, self.node = self.token, self.token + "-node"
        self.container_name = "buildx_buildkit_" + self.node
        self.volume_name = self.container_name + "_state"
        self.tag = self.token + ":builder"
        self.mapping = None
        self.image_id = self.container_id = self.volume = None
        self.registered = self.prepared = self.effects = False
        self.verifications = 0
        self.invocations = []
        self.inventory = None

    def command(self, label, args, mutate=False, **kwargs):
        call = self.harness.mutate if mutate else self.harness.docker
        return call("builder-" + label, self.descriptor, args, **kwargs)

    def object(self, kind, name):
        raw, _, _ = self.command(kind + "-inspect", [kind, "inspect", name])
        items = image_input.parse(raw)
        require(isinstance(items, list) and len(items) == 1, "ambiguous builder object")
        return items[0]

    def absent(self, kind, name):
        args = [kind, "ls", "--quiet"]
        if kind == "container":
            args += ["--all", "--no-trunc", "--filter", "name=^/" + name + "$"]
        elif kind == "image":
            args += ["--filter", "reference=" + name]
        else:
            args += ["--filter", "name=^" + name + "$"]
        raw, _, _ = self.command(kind + "-absent", args)
        require(not raw.strip(), "candidate builder object preexists or remains after removal")

    def engine_guard(self):
        raw, _, _ = self.command("engine", ["info", "--format", "{{json .}}"])
        info = image_input.parse(raw)
        require(info["ID"] == self.descriptor["engine_id"] and info["OSType"] == "linux" and
                info["Architecture"] in ("aarch64", "arm64") and info["DefaultRuntime"] == "youki" and
                info["Runtimes"]["youki"]["path"] == "/mnt/linux-bin/youki", "builder Engine/context/runtime mismatch")

    def inspect_owned(self, running=True):
        self.engine_guard()
        image = self.object("image", self.tag)
        require(image["Id"] == self.image_id and image["Architecture"] == "arm64" and image["Os"] == "linux" and
                image["Config"]["Labels"] == {LABEL: self.token} and
                image["Config"]["Entrypoint"] == ["/usr/bin/buildkitd"], "builder image ownership/configuration drift")
        volume = volume_identity(self.object("volume", self.volume_name), self.volume_name, self.token)
        require(volume == self.volume, "builder cache volume replaced")
        item = self.object("container", self.container_id)
        require(item["Id"] == self.container_id and item["Name"] == "/" + self.container_name and
                item["Image"] == self.image_id and item["Config"]["Labels"] == {LABEL: self.token} and
                item["Config"]["Entrypoint"] == ["/usr/bin/buildkitd"] and item["Config"]["Cmd"] == FLAGS,
                "builder container identity/configuration drift")
        host = item["HostConfig"]
        require(host["Runtime"] == "youki" and host["Privileged"] is True and host["Init"] is True and
                host["NetworkMode"] == "bridge" and not host.get("Binds") and not host.get("PortBindings") and
                host["RestartPolicy"]["Name"] == "no", "builder execution/network policy drift")
        mounts = item["Mounts"]
        require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["Name"] == self.volume_name and
                mounts[0]["Destination"] == "/var/lib/buildkit" and mounts[0]["Source"] == self.volume["Mountpoint"] and
                mounts[0]["RW"] is True, "builder cache mount is not exact owned volume")
        require(item["State"]["Running"] is running and item["RestartCount"] == 0, "builder lifecycle drift")
        return item

    def prepare(self):
        require(not self.effects, "builder provisioning is single-attempt")
        self.engine_guard()
        for kind, name in (("image", self.tag), ("container", self.container_name), ("volume", self.volume_name)):
            self.absent(kind, name)
        raw, _, _ = self.command("names", ["buildx", "ls", "--format", "{{.Name}}"])
        require(self.name not in [line.strip().rstrip("*") for line in raw.decode().splitlines()],
                "builder name preexists")
        payload, self.inventory = rootfs_payload(self.harness)
        rootfs = self.harness.evidence / (self.token + "-rootfs.tar")
        startup.write(rootfs, payload)
        startup.document(self.harness.evidence / (self.token + "-image-input.json"),
                         {"owner": self.descriptor["owner"], "context": self.descriptor["name"],
                          "rootfs_archive": rootfs.name, "rootfs_sha256": sha(payload), "inventory": self.inventory,
                          "buildkit": self.harness.info["buildkit"], "worker_flags": FLAGS})
        self.effects = True
        with rootfs.open("rb") as source:
            raw, _, _ = self.command("image-import", ["image", "import", "--platform", "linux/arm64",
                "--change", "ENTRYPOINT [\"/usr/bin/buildkitd\"]", "--change", "ENV PATH=/usr/bin:/bin",
                "--change", "ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
                "--change", "LABEL " + LABEL + "=" + self.token, "-", self.tag], mutate=True, stdin=source, timeout=300)
        require(startup.digest(rootfs) == sha(payload), "retained builder rootfs changed during import")
        self.image_id = startup.image_id(raw)
        self.command("volume-create", ["volume", "create", "--label", LABEL + "=" + self.token, self.volume_name], mutate=True)
        self.volume = volume_identity(self.object("volume", self.volume_name), self.volume_name, self.token)
        raw, _, _ = self.command("container-create", ["container", "create", "--name", self.container_name,
            "--label", LABEL + "=" + self.token, "--privileged", "--init", "--network", "bridge", "--restart", "no",
            "--mount", "type=volume,source=" + self.volume_name + ",target=/var/lib/buildkit",
            self.image_id, *FLAGS], mutate=True)
        self.container_id = driver.checked_text(raw.decode().strip(), r"[0-9a-f]{64}", "builder container ID")
        self.inspect_owned(running=False)
        self.command("container-start", ["container", "start", self.container_id], mutate=True)
        self.inspect_owned()
        self.command("register", ["buildx", "create", "--name", self.name, "--node", self.node,
            "--driver", "docker-container", "--driver-opt", "image=" + self.image_id,
            "--buildkitd-flags", " ".join(FLAGS), self.descriptor["name"]], mutate=True)
        self.registered = True
        raw, _, _ = self.command("bootstrap", ["buildx", "inspect", "--bootstrap", self.name], mutate=True, timeout=120)
        self.mapping = {"name": self.name, "node": self.node, "container_id": self.container_id, "image_id": self.image_id}
        driver.assert_builder_inspect(raw, self.mapping, self.descriptor["name"])
        self.prepared = True
        self.verify(require_invocation=False)
        return dict(self.mapping)

    def verify(self, require_invocation=True):
        require(self.prepared, "builder readiness incomplete")
        self.inspect_owned()
        raw, _, _ = self.command("inspect", ["buildx", "inspect", self.name])
        driver.assert_builder_inspect(raw, self.mapping, self.descriptor["name"])
        script = '''set -eu
/bin/busybox find / -xdev \\( -name runc -o -name crun -o -name buildkit-runc \\) -print
/bin/busybox find /var/lib/buildkit -xdev \\( -name runc -o -name crun -o -name buildkit-runc \\) -print
'''
        raw, _, _ = self.command("forbidden-runtime-inventory", ["exec", self.container_id, "/bin/busybox", "sh", "-c", script])
        require(not raw.strip(), "forbidden OCI runtime in builder or worker cache")
        paths = ["/" + name for name in sorted(self.inventory)]
        raw, _, _ = self.command("runtime-hashes", ["exec", self.container_id, "/bin/busybox", "sha256sum", *paths])
        observed = {}
        for line in raw.decode().splitlines():
            digest, path = line.split("  ", 1)
            require(path not in observed and re.fullmatch(r"[0-9a-f]{64}", digest), "invalid runtime hash inventory")
            observed[path] = digest
        require(observed == {"/" + name: entry["sha256"] for name, entry in self.inventory.items()}, "builder payload changed")
        raw, _, _ = self.command("workers", ["exec", self.container_id, "/usr/bin/buildctl", "debug", "workers", "--verbose"])
        require(len(re.findall(rb"(?m)^ID:\s+\S+", raw)) == 1 and
                len(re.findall(rb"org\.mobyproject\.buildkit\.worker\.executor:\s+oci", raw)) == 1,
                "exactly one OCI worker required")
        raw, _, _ = self.command("youki-invocations", ["exec", self.container_id, "/bin/busybox", "sh", "-c",
            'if test -f /var/lib/buildkit/vz-youki-invocations.log; then /bin/busybox cat /var/lib/buildkit/vz-youki-invocations.log; fi'])
        invocations = raw.decode().splitlines()
        require(all(re.fullmatch(r"vz-youki-invocation pid=[1-9][0-9]*", line) for line in invocations) and
                invocations[:len(self.invocations)] == self.invocations, "runtime invocation history changed")
        if require_invocation:
            require(len(invocations) > len(self.invocations), "workloads lack new pinned-youki worker invocation evidence")
        self.invocations = invocations
        self.verifications += 1
        proof = {"scope": "owned_buildkit_builder_and_worker_cache_not_full_release_runtime_attestation",
                 "owner": self.descriptor["owner"], "context": self.descriptor["name"], "engine_id": self.descriptor["engine_id"],
                 "builder": self.mapping, "cache_volume": self.volume, "runtime_hashes": observed,
                 "youki_invocations": invocations, "post_workload": require_invocation}
        startup.document(self.harness.evidence / f"{self.token}-runtime-{self.verifications:03}.json", proof)
        return proof

    def remove_owned(self):
        if not self.effects:
            return
        self.harness.assert_certain()
        require(self.prepared and self.registered, "partially provisioned builder retained for explicit reconciliation")
        self.inspect_owned()
        self.command("unregister", ["buildx", "rm", "--keep-daemon", "--keep-state", self.name], mutate=True)
        self.registered = False
        raw, _, _ = self.command("unregistered", ["buildx", "ls", "--format", "{{.Name}}"])
        require(self.name not in [line.strip().rstrip("*") for line in raw.decode().splitlines()], "builder registration remains")
        self.inspect_owned()
        self.command("container-stop", ["container", "stop", "--time", "30", self.container_id], mutate=True, timeout=60)
        item = self.inspect_owned(running=False)
        require(item["State"]["Status"] == "exited" and item["State"]["ExitCode"] == 0,
                "builder did not gracefully exit; cleanup withheld")
        self.command("container-remove", ["container", "rm", self.container_id], mutate=True)
        self.absent("container", self.container_name)
        raw, _, _ = self.command("container-id-absent", ["container", "ls", "--all", "--quiet", "--no-trunc", "--filter", "id=" + self.container_id])
        require(not raw.strip(), "exact builder container remains after removal")
        require(volume_identity(self.object("volume", self.volume_name), self.volume_name, self.token) == self.volume,
                "cache volume replaced before removal")
        self.command("volume-remove", ["volume", "rm", self.volume_name], mutate=True)
        self.absent("volume", self.volume_name)
        image = self.object("image", self.tag)
        require(image["Id"] == self.image_id and image["Config"]["Labels"] == {LABEL: self.token}, "builder image replaced")
        self.command("image-remove", ["image", "rm", self.tag], mutate=True)
        self.absent("image", self.tag)
        raw, _, _ = self.command("image-id-absent", ["image", "ls", "--all", "--quiet", "--no-trunc"])
        require(self.image_id not in raw.decode().split(), "builder image remains under another reference")
        startup.document(self.harness.evidence / (self.token + "-cleanup.json"),
                         {"builder": self.mapping, "cache_volume": self.volume, "owner": self.descriptor["owner"],
                          "buildx_registration_absent": True,
                          "exact_owned_builder_container_volume_image_removed": True})
