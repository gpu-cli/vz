#!/usr/bin/env python3
"""Fresh installed public-CLI Delete happy-path DEV proof, not 0.4 certification.

No existing-runtime attachment, retries, registry pulls, global Docker routing,
forced daemon termination, or recursive host cleanup is supported.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
import uuid

import installed_developer_startup as startup
from installed_delete_quiescence import validate as validate_quiescence
from linux_docker_e2e import public_activation

require = startup.require
SCOPE = "DEV_INSTALLED_PUBLIC_DELETE_HAPPY_PATH_NOT_RELEASE_CERTIFICATION"
LABEL = "dev.vz.installed-delete-proof"
REPO = Path(__file__).resolve().parents[2]


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for name in startup.OPTIONS:
        require(sum(x == "--" + name or x.startswith("--" + name + "=") for x in argv) <= 1,
                "duplicate option: --" + name)
        parser.add_argument("--" + name, required=True)
    return parser.parse_args(argv)


def preflight(args, require_host=True):
    info = startup.preflight(args, require_host=require_host)
    info["scope"] = SCOPE
    for name in ("scripts/helpers/installed_delete_e2e.py", "scripts/run-installed-delete-e2e.sh",
                 "scripts/helpers/installed_delete_quiescence.py",
                 "scripts/helpers/linux_docker_e2e.py", "scripts/helpers/linux_docker_image_input.py",
                 "crates/vz-cli/tests/fixtures/dev-help.txt"):
        path = REPO / name
        info["inputs"][str(path)] = startup.digest(path)
    return info


def resource_name(owner, kind, logical):
    raw = b"vz.resource-name.v1\x00"
    for field in (owner["project_id"], owner["environment_id"], owner["machine_id"], kind, logical):
        encoded = field.encode("ascii")
        raw += len(encoded).to_bytes(8, "little") + encoded
    readable = re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9_.]", "-", kind + "-" + logical)).strip("-")[:26]
    return "vzr1-" + readable + "-" + hashlib.sha256(raw).hexdigest()[:32]


def ownership(owner, kind, resource):
    # Rust's typed OwnershipRecord field order is part of its digest encoding.
    return {"schema_version": 1, "resource_kind": kind, "resource_id": resource,
            "environment_id": owner["environment_id"], "machine_id": owner["machine_id"]}


def ownership_key(row):
    kind = row["resource_kind"]
    return ("other:" + kind["other"] if isinstance(kind, dict) else kind,
            row["resource_id"], row["machine_id"])


def ownership_digest(rows):
    ordered = [ownership({"environment_id": x["environment_id"], "machine_id": x["machine_id"]},
                         x["resource_kind"], x["resource_id"]) for x in sorted(rows, key=ownership_key)]
    return "sha256:" + hashlib.sha256(json.dumps(ordered, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def identity(path):
    require(path.resolve(strict=True) == path, "redirected physical identity")
    value = path.lstat()
    require(path.is_dir() and value.st_uid == os.geteuid(), "owned directory identity required")
    return {"device": value.st_dev, "inode": value.st_ino}


def bounded_receipt_paths(root, limit=4096):
    """Bound entries while enumerating, including each directory itself.

    scandir stays lazy even for a single overlarge directory; pathlib.rglob
    implementations can internally materialize a directory before yielding.
    Symlinks are rejected before either processing or descending into them.
    """
    pending, observed = [root], 0
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                observed += 1
                require(observed <= limit, "unexpected runtime store receipt inventory")
                require(not entry.is_symlink(), "redirected runtime evidence")
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                yield path


def request_hash(project_id, environment_id, selector):
    value = ["delete", project_id, environment_id, {"explicit": {"kind": "name_or_id", "value": selector}}, 120000]
    return "sha256:" + hashlib.sha256(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def runtime_inventory(inventory, owner, incarnation, youki_digest):
    require(inventory["owner"] == owner and inventory["incarnation"] == incarnation and
            inventory["youki_sha256"] == youki_digest and
            inventory["scope"] == "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
            "runtime inventory owner, incarnation, youki or bounded scope differs")
    stdout = inventory["stdout"]
    header = "vz-startup-runtime-inventory-v1\nyouki-sha256=" + youki_digest + "\n"
    marker = "alternate-runtime-binaries=absent\n"
    require(isinstance(stdout, str) and len(stdout.encode("utf-8")) <= 8192 and
            stdout.startswith(header) and stdout.endswith(marker) and
            stdout.count("alternate-runtime-binaries=") == 1 and
            stdout.count("vz-startup-runtime-inventory-v1\n") == 1 and stdout.count("youki-sha256=") == 1,
            "runtime inventory lacks exact bounded youki header and final alternate-binary absence proof")


def delete_terminal(raw, environment, bindings, request, selector):
    rows = [json.loads(line) for line in raw.splitlines()]
    require(rows and rows[0] == {"schema_version": 1, "record_type": "request_started",
            "operation": "delete_environment", "request_id": request, "idempotency_key": request},
            "Delete omitted exact replay preamble")
    events = rows[1:]
    require(events and all(x.get("record_type") == "operation_progress" for x in events), "unknown Delete record")
    require(sum(x.get("terminal") is True for x in events) == 1 and events[-1]["terminal"] is True,
            "exact final Delete terminal required")
    wanted = {b["machine"]["machine_id"]: b for b in bindings}
    expected_ownership = [row for b in bindings for row in b["ownership"]]
    immutable, sequence = None, -1
    for event in events:
        operation = event["operation"]
        require(event["schema_version"] == 1 and event["request_id"] == event["idempotency_key"] == request and
                event.get("error") is None and type(event["sequence"]) is int and event["sequence"] > sequence,
                "uncorrelated/error/nonmonotonic Delete progress")
        sequence = event["sequence"]
        require(operation["schema_version"] == 1 and operation["kind"] == "delete" and
                operation["project_id"] == environment["project_id"] and
                operation["environment_id"] == environment["environment_id"] and
                operation["request_id"] == operation["idempotency_key"] == request and
                operation["definition_digest"] == environment["definition_digest"] and
                operation["generation"] == environment["lifecycle_generation"] + 1 and
                operation["initial_state"] == environment["state"] and operation["requested_target"] == "deleted",
                "Delete operation changed original Environment authority")
        require(operation["request_hash"] == request_hash(environment["project_id"], environment["environment_id"], selector),
                "Delete request hash differs from exact CLI selector/timeout")
        steps = operation["machine_steps"]
        require(len(steps) == len(wanted) and {x["machine_id"] for x in steps} == set(wanted), "Delete Machine set changed")
        for step in steps:
            binding = wanted[step["machine_id"]]
            require(step["initial_state"] == next(x["state"] for x in environment["machines"] if x["machine_id"] == step["machine_id"]) and
                    step.get("target_state") is None and step.get("resulting_incarnation") is None and
                    step.get("resulting_activation") is None and step["expected_incarnation"] == binding["incarnation"],
                    "Delete Machine incarnation or action changed")
        cleanups = operation["cleanup_steps"]
        require(len(cleanups) == len(expected_ownership) and
                sorted((x["ownership"] for x in cleanups), key=ownership_key) == sorted(expected_ownership, key=ownership_key),
                "Delete cleanup ownership graph changed")
        scope = {key: operation[key] for key in ("operation_id", "request_hash", "generation", "definition_digest")}
        require(immutable is None or immutable == scope, "Delete stream immutable scope changed")
        immutable = scope
    result = events[-1]
    operation, tombstone = result["operation"], result["tombstone"]
    require(operation["status"] == "succeeded" and all(x["status"] == "succeeded" and x.get("failure_reason") is None
            for x in operation["machine_steps"] + operation["cleanup_steps"]), "Delete lacks exact positive cleanup acknowledgements")
    require(tombstone["schema_version"] == 1 and
            all(tombstone[k] == environment[k] for k in ("project_id", "environment_id", "definition_digest", "name")) and
            tombstone["delete_operation_id"] == operation["operation_id"] and
            tombstone["lifecycle_generation"] == operation["generation"] and
            tombstone["deleted_at"] == operation["completed_at"] and type(tombstone["deleted_at"]) is int and
            tombstone["ownership_digest"] == ownership_digest(expected_ownership), "tombstone does not bind the exact completed ownership graph")
    return {"operation": operation, "tombstone": tombstone, "request_id": request}


class NeighborMonitor:
    """Repeated bounded observations, not a claim of packet-level zero downtime."""
    def __init__(self, harness, sentinels):
        self.harness, self.sentinels = harness, sentinels
        self.directory = startup.private(harness.evidence / "neighbor-liveness")
        self.record = startup.Recorder(self.directory, harness.env)
        self.stop_event, self.first = threading.Event(), threading.Event()
        self.samples, self.errors = [], []
        self.thread = threading.Thread(target=self.loop, name="vz-delete-neighbor-liveness", daemon=False)

    def command(self, descriptor, args):
        return self.record.run("neighbor", ["docker", "--config", str(self.harness.config), "--context", descriptor["name"], *args],
            executable=self.harness.info["clients"]["docker"]["canonical"], cwd=self.harness.root, timeout=20)

    def loop(self):
        try:
            while not self.stop_event.is_set():
                for row in self.sentinels:
                    if self.stop_event.is_set():
                        return
                    begin = time.time_ns()
                    self.harness.check_sentinel(row, command=self.command)
                    self.samples.append({"machine_id": row["descriptor"]["owner"]["machine_id"],
                                         "started_unix_ns": begin, "completed_unix_ns": time.time_ns()})
                self.first.set()
                self.stop_event.wait(0.5)
        except BaseException as error:
            self.errors.append(f"{type(error).__name__}: {error}")
            self.first.set()

    def start(self):
        self.thread.start()
        require(self.first.wait(130), "neighbor initial sample deadline exceeded")
        self.check()

    def check(self):
        require(not self.errors and self.thread.is_alive(), "neighbor liveness failed: " + repr(self.errors))

    def finish(self):
        self.stop_event.set()
        # One sample has four bounded commands; allow independent observer
        # termination without ever signalling the daemon or a Machine process.
        deadline = time.monotonic() + 90
        while self.thread.is_alive() and time.monotonic() < deadline:
            self.thread.join(timeout=min(20, max(0, deadline - time.monotonic())))
        require(not self.thread.is_alive(), "neighbor observer remains active; lifecycle cleanup withheld")
        startup.document(self.directory / "samples.json", {"samples": self.samples, "errors": self.errors,
            "scope": "sampled_Engine_identity_nonrestarted_container_and_volume_bytes_not_zero_downtime_certification", "retries": 0})
        require(not self.errors, "neighbor observations failed: " + repr(self.errors))


class DeleteHarness(startup.Harness):
    def __init__(self, info):
        super().__init__(info)
        self.unresolved_deletes = set()
        self.monitor = None
        self.deleted = []
        self.host_files = {}

    def check_defaults(self):
        require(startup.digest(self.config / "config.json") == self.default_digest, "isolated Docker default bytes changed")
        require({str(path): startup.snapshot_config(path) for path in self.baseline_paths} == self.baseline,
                "daily Docker default bytes changed")
        require(all(startup.digest(Path(path)) == expected for path, expected in self.host_files.items()),
                "host project, Git worktree identity, or user sentinel changed")

    def capture_host_files(self, project):
        raw, stderr, _ = self.command("worktree-metadata", ["/usr/bin/git", "rev-parse", "--path-format=absolute", "--git-dir"], cwd=project)
        require(not stderr, "Git metadata resolution emitted stderr")
        directory = startup.canonical(raw.decode().strip())
        require(directory.is_relative_to(self.root), "worktree metadata escaped fresh fixture")
        token = directory / "vz/workspace-id"
        require(re.fullmatch(rb"wsp_[0-9a-f]{32}", startup.read_private_regular(token, 128)), "public Up workspace token missing")
        paths = [project / "vz.json", project / ".git", project / "user-sentinel.txt", directory / "HEAD", token]
        self.host_files = {str(path): startup.digest(path) for path in paths}
        startup.document(self.evidence / "preserved-host-files.json", self.host_files)

    def public_inventory(self):
        raw, stderr, _ = self.command("installed-help", [self.cli, "--help"])
        require(not stderr, "help emitted stderr")
        section = raw.decode().split("\nCommands:\n", 1)[1].split("\nOptions:\n", 1)[0]
        verbs = [line.split()[0] for line in section.splitlines() if line.strip()]
        require(len(verbs) == 6 and set(verbs) == {"up", "exec", "status", "stop", "delete", "help"}, "installed parser is not exactly the five lifecycle verbs")
        _, _, code = self.command("retired-vm-rejected", [self.cli, "vm", "list"], success=False)
        require(code != 0 and not os.path.lexists(self.socket), "retired vm dispatched or started daemon")

    def bind_environment(self, environment):
        bindings = []
        capture_root = startup.private(self.evidence / ("before-" + environment["environment_id"]))
        for machine in environment["machines"]:
            descriptor = startup.context_descriptor(environment, machine, self.config)
            owner = descriptor["owner"]
            key = resource_name(owner, "other:machine_runtime_store", "runtime")
            store = self.runtime / "topology-machines" / key
            store_identity, data_identity = identity(store), identity(store / "data")
            manifest_bytes = startup.read_private_regular(store / "owner.json", startup.LIMIT)
            manifest = json.loads(manifest_bytes)
            expected_reservation = ownership(owner, {"other": "machine_runtime_store"}, key)
            require(manifest["schema_version"] == 1 and manifest["owner"] == owner and
                    manifest["reservation"] == expected_reservation, "foreign store manifest")
            configuration_bytes = startup.read_private_regular(store / "data/linux-target/configuration.json", startup.LIMIT)
            configuration = json.loads(configuration_bytes)
            require(json.dumps(configuration, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() == configuration_bytes and
                    manifest["configuration_digest"] == "sha256:" + hashlib.sha256(b"vz.machine-configuration.v1\x00" + configuration_bytes).hexdigest(),
                    "Machine configuration bytes do not bind admitted digest")
            require(configuration["release_version"] == self.info["release_version"] and configuration["kernel_profile"] == "developer" and
                    configuration["machine"]["target"] == machine["target"] and configuration["machine"]["name"] == machine["name"] and
                    configuration["machine"]["profile"] == "developer" and configuration["backend"] == machine["backend"],
                    "physical configuration differs from public selected Machine")
            for key_name, filename in (("kernel_sha256", "vmlinux"), ("initramfs_sha256", "initramfs.img"),
                                       ("version_sha256", "version.json"), ("youki_sha256", "youki")):
                require(configuration["artifact"][key_name] == startup.digest(self.prefix / "linux/developer" / filename),
                        "Machine used another installed artifact: " + filename)
            runtime = public_activation(self, environment, machine)
            activation = None
            # The public helper authenticated exactly one matching Up completion;
            # take its complete incarnation, including original creation metadata.
            for command in self.record.receipts:
                if command["label"] != "public-up":
                    continue
                raw = startup.read_private_regular(self.evidence / f'{command["index"]:03}-public-up.stdout', startup.LIMIT)
                for line in raw.splitlines():
                    completion = json.loads(line).get("progress", {}).get("completion")
                    if completion and completion["admission"]["environment_id"] == environment["environment_id"]:
                        for step in completion["operation"]["machine_steps"]:
                            if step["machine_id"] == machine["machine_id"]:
                                activation = step["resulting_activation"]
            require(activation is not None and activation["runtime_identity"] == runtime, "complete public activation missing")
            capture = startup.private(capture_root / machine["machine_id"])
            matches = []
            for path in bounded_receipt_paths(store):
                require(not path.is_symlink(), "redirected runtime evidence")
                if path.is_file() and path.suffix in (".json", ".log", ".stdout", ".stderr"):
                    raw = startup.read_private_regular(path, 32 * 1024 * 1024)
                    destination = capture / path.relative_to(store)
                    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    startup.write(destination, raw)
                    if path.name == "receipt.json":
                        receipt = json.loads(raw)
                        if receipt.get("owner") == owner and receipt.get("incarnation") == activation["incarnation"]:
                            matches.append((path, raw, receipt))
            require(len(matches) == 1, "exact current startup receipt required")
            path, raw, receipt = matches[0]
            require(receipt["schema_version"] == 1 and receipt["state"] == "completed" and receipt["failure"] is None and
                    receipt["configuration_digest"] == manifest["configuration_digest"] and receipt["context"] == descriptor["name"] and
                    receipt["resources"]["engine_id"] == descriptor["engine_id"] and
                    receipt["archive_sha256"] == startup.digest(self.prefix / "linux/developer/developer-probe-rootfs.tar") and
                    receipt["client_sha256"] == self.info["clients"]["docker"]["sha256"], "startup proof does not bind current Machine/client/probe")
            after = json.loads(startup.read_private_regular(path.parent / "runtime-inventory-after.json", startup.LIMIT))
            require(after["probe_receipt_sha256"] == hashlib.sha256(raw).hexdigest() and
                    after["runtime_inventory"]["owner"] == owner and after["runtime_inventory"]["incarnation"] == activation["incarnation"] and
                    after["runtime_inventory"]["youki_sha256"] == startup.digest(self.prefix / "linux/developer/youki"), "after-probe runtime inventory changed")
            youki_digest = startup.digest(self.prefix / "linux/developer/youki")
            runtime_inventory(receipt["resources"]["runtime_inventory"], owner, activation["incarnation"], youki_digest)
            runtime_inventory(after["runtime_inventory"], owner, activation["incarnation"], youki_digest)
            context_key = hashlib.sha256(descriptor["name"].encode()).hexdigest()
            rows = [ownership(owner, "machine", machine["machine_id"]), expected_reservation,
                    ownership(owner, {"other": "runtime_vm"}, json.loads(runtime["opaque_id"])["stack_id"]),
                    ownership(owner, "incarnation", machine["incarnation_id"]), ownership(owner, "docker_context", descriptor["name"])]
            bindings.append({"machine": machine, "owner": owner, "descriptor": descriptor, "incarnation": activation["incarnation"],
                "runtime_identity": runtime, "manifest": manifest, "store_path": str(store), "store_identity": store_identity,
                "data_identity": data_identity, "context_path": str(self.config / "contexts/meta" / context_key),
                "tls_path": str(self.config / "contexts/tls" / context_key), "ownership": rows})
        startup.document(self.evidence / (environment["environment_id"] + "-physical-bindings.json"), bindings)
        return bindings

    def sentinel(self, descriptor):
        token = "vzdel-" + uuid.uuid4().hex[:24]
        label = LABEL + "=" + token
        tag, volume = token + ":rootfs", token + "-volume"
        for kind, args in (("container", ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + token + "$"]),
                           ("volume", ["volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"]),
                           ("image", ["image", "ls", "--quiet", "--filter", "reference=" + tag])):
            raw, _, _ = self.docker("sentinel-" + kind + "-absent", descriptor, args)
            require(not raw.strip(), "fresh sentinel name already exists")
        with (self.prefix / "linux/developer/developer-probe-rootfs.tar").open("rb") as stream:
            raw, _, _ = self.docker("sentinel-import", descriptor, ["image", "import", "--change", "LABEL " + label, "-", tag], stdin=stream)
        image = startup.image_id(raw)
        raw, _, _ = self.docker("sentinel-volume", descriptor, ["volume", "create", "--label", label, volume])
        require(raw.decode().strip() == volume, "created volume changed name")
        raw, _, _ = self.docker("sentinel-run", descriptor, ["run", "--pull", "never", "--network", "none", "--detach",
            "--label", label, "--name", token, "--mount", "type=volume,src=" + volume + ",dst=/sentinel",
            image, "/bin/sleep", "3600"])
        cid = raw.decode().strip()
        require(re.fullmatch(r"[0-9a-f]{64}", cid), "exact sentinel container ID missing")
        raw, _, _ = self.docker("sentinel-created-inspect", descriptor, ["container", "inspect", cid])
        inspected = json.loads(raw)
        require(len(inspected) == 1 and inspected[0]["Id"] == cid and inspected[0]["Image"] == image and
                inspected[0]["Config"]["Labels"][LABEL] == token, "exact owned sentinel inspection required")
        self.docker("sentinel-write-volume", descriptor, ["exec", cid, "/bin/sh", "-c",
            'printf "%s\\n" "$1" > /sentinel/value', "sh", token])
        row = {"descriptor": descriptor, "token": token, "container_id": cid, "image_id": image,
               "volume": volume, "started_at": inspected[0]["State"]["StartedAt"]}
        self.check_sentinel(row)
        return row

    def check_sentinel(self, row, command=None):
        descriptor = row["descriptor"]
        command = command or (lambda context, args: self.docker("sentinel-observe", context, args, timeout=20))
        raw, _, _ = command(descriptor, ["info", "--format", "{{.ID}}"])
        require(raw.decode().strip() == descriptor["engine_id"], "neighbor Engine changed")
        raw, _, _ = command(descriptor, ["container", "inspect", row["container_id"]])
        items = json.loads(raw)
        require(len(items) == 1, "exact sentinel container required")
        item = items[0]
        require(item["Id"] == row["container_id"] and item["Image"] == row["image_id"] and item["State"]["Running"] and
                item["State"]["StartedAt"] == row["started_at"] and item["RestartCount"] == 0 and
                item["HostConfig"]["Runtime"] == "youki" and item["Config"]["Labels"][LABEL] == row["token"] and
                any(x["Type"] == "volume" and x["Name"] == row["volume"] and x["Destination"] == "/sentinel" for x in item["Mounts"]),
                "sentinel stopped/restarted/replaced or volume changed")
        raw, _, _ = command(descriptor, ["volume", "inspect", row["volume"]])
        volumes = json.loads(raw)
        require(len(volumes) == 1 and volumes[0]["Name"] == row["volume"] and volumes[0]["Labels"][LABEL] == row["token"], "sentinel volume ownership changed")
        raw, stderr, _ = command(descriptor, ["exec", row["container_id"], "/bin/cat", "/sentinel/value"])
        require(raw == (row["token"] + "\n").encode() and not stderr, "sentinel volume bytes changed")

    def verify_deleted(self, bindings, result):
        for binding in bindings:
            for path in (binding["store_path"], binding["context_path"], binding["tls_path"], binding["descriptor"]["endpoint"][7:]):
                require(not os.path.lexists(path), "deleted owned path remains: " + path)
            directory = self.runtime / "topology-machine-deletions" / Path(binding["store_path"]).name
            identity(directory)
            raw = startup.read_private_regular(directory / "intent.json", startup.LIMIT)
            intent = json.loads(raw)
            receipt = json.loads(startup.read_private_regular(directory / "receipt.json", startup.LIMIT))
            operation = result["operation"]
            validate_quiescence(binding, operation, intent["quiescence"])
            require(intent["schema_version"] == receipt["schema_version"] == 1 and intent["manifest"] == binding["manifest"] and
                    intent["operation"]["operation_id"] == receipt["operation_id"] == operation["operation_id"] and
                    intent["operation"]["generation"] == receipt["generation"] == operation["generation"] and
                    intent["operation"]["request_hash"] == operation["request_hash"] and
                    intent["operation"]["definition_digest"] == operation["definition_digest"] and
                    receipt["owner"] == binding["owner"] and receipt["configuration_digest"] == binding["manifest"]["configuration_digest"] and
                    intent["store"] == receipt["store"] == binding["store_identity"] and
                    intent["data"] == receipt["data"] == binding["data_identity"] and
                    receipt["intent_sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest() and receipt["store_removed"] is True and
                    intent["quiescence"]["owner"] == binding["owner"] and not os.path.lexists(directory / "store"),
                    "outside-tree deletion receipt does not bind exact original store")
            _, _, code = self.docker("deleted-context-rejected", binding["descriptor"], ["info", "--format", "{{.ID}}"], timeout=15, success=False)
            require(code != 0, "deleted context still reaches an Engine")
        self.check_defaults()

    def delete(self, project, selector, environment, bindings, request=None):
        request = request or "delete-" + uuid.uuid4().hex
        before = self.observe_neighbors()
        self.unresolved_deletes.add(request)
        started = time.time_ns()
        raw, stderr, code = self.command("public-delete", [self.cli, "--json", "delete", "--environment", selector,
            "--timeout", "120", "--request-id", request, "--idempotency-key", request], cwd=project, timeout=420, success=False)
        require(code == 0 and not stderr, "public Delete failed; retain exact operation and runtime for reconciliation")
        result = delete_terminal(raw, environment, bindings, request, selector)
        self.verify_deleted(bindings, result)
        self.unresolved_deletes.remove(request)
        target = (project, environment["environment_id"])
        if target in self.cleanup_targets:
            self.cleanup_targets.remove(target)
        result["observed_interval"] = [started, time.time_ns()]
        after = self.observe_neighbors()
        overlapping = [] if self.monitor is None else [x for x in self.monitor.samples
            if x["started_unix_ns"] <= result["observed_interval"][1] and x["completed_unix_ns"] >= started]
        result["neighbor_observations"] = {"before": before, "after": after, "background_overlap": overlapping,
            "background_overlap_missing_for": [x["machine_id"] for x in before
                if not any(y["machine_id"] == x["machine_id"] for y in overlapping)],
            "scope": "positive_before_after_and_repeated_background_samples_not_zero_downtime"}
        self.deleted.append(result)
        return result

    def observe_neighbors(self):
        if self.monitor is None:
            return []
        self.monitor.check()
        result = []
        for row in self.monitor.sentinels:
            begin = time.time_ns()
            self.check_sentinel(row)
            result.append({"machine_id": row["descriptor"]["owner"]["machine_id"],
                           "started_unix_ns": begin, "completed_unix_ns": time.time_ns()})
        return result

    def scenario(self):
        self.public_inventory()
        project = self.project("developer-delete", "developer", 2)
        startup.write(project / "user-sentinel.txt", ("host-user-data-" + uuid.uuid4().hex + "\n").encode())
        primary = self.up(project, "primary")
        self.capture_host_files(project)
        self.daemon_identity = self.daemon_fingerprint()
        primary_contexts = self.inspect(primary)
        primary_bindings = self.bind_environment(primary)
        neighbor = self.up(project, "neighbor")
        startup.exact_developer_topology(primary, neighbor)
        neighbor_contexts = self.inspect(neighbor)
        neighbor_bindings = self.bind_environment(neighbor)
        for field in ("name", "endpoint", "engine_id"):
            require(len({x[field] for x in primary_contexts + neighbor_contexts}) == 4, "Machines share " + field)
        primary_workloads = [self.sentinel(x) for x in primary_contexts]
        sentinels = [self.sentinel(x) for x in neighbor_contexts]
        self.monitor = NeighborMonitor(self, sentinels)
        self.monitor.start()
        ready_delete = self.delete(project, "primary", primary, primary_bindings)
        self.monitor.check()
        for row in sentinels:
            self.check_sentinel(row)
        recreated = self.up(project, "primary")
        recreated_contexts = self.inspect(recreated)
        recreated_bindings = self.bind_environment(recreated)
        require(recreated["environment_id"] != primary["environment_id"] and
                {x["machine_id"] for x in recreated["machines"]}.isdisjoint(x["machine_id"] for x in primary["machines"]) and
                {x["name"] for x in recreated_contexts}.isdisjoint(x["name"] for x in primary_contexts),
                "name reuse adopted deleted immutable identities")
        replay = self.delete(project, "primary", primary, primary_bindings, ready_delete["request_id"])
        require(replay["operation"] == ready_delete["operation"] and replay["tombstone"] == ready_delete["tombstone"], "old request replay changed tombstone")
        require(self.status(project, recreated["environment_id"]) == recreated and self.inspect(recreated) == recreated_contexts,
                "old Delete replay affected replacement Environment")
        self.monitor.check()
        stopped = self.stop(project, recreated["environment_id"])
        stopped_delete = self.delete(project, recreated["environment_id"], stopped, recreated_bindings)
        require(self.status(project, neighbor["environment_id"]) == neighbor and self.inspect(neighbor) == neighbor_contexts,
                "neighbor topology changed during primary lifecycle")
        for row in sentinels:
            self.check_sentinel(row)
        self.monitor.finish()
        monitor_summary = {"samples": self.monitor.samples, "errors": self.monitor.errors}
        self.monitor = None
        for row in sentinels:
            require(any(x["machine_id"] == row["descriptor"]["owner"]["machine_id"] and
                        x["started_unix_ns"] <= stopped_delete["observed_interval"][1] and
                        x["completed_unix_ns"] >= ready_delete["observed_interval"][0] for x in monitor_summary["samples"]),
                    "no neighbor observation overlaps primary lifecycle")
        stopped_neighbor = self.stop(project, neighbor["environment_id"])
        neighbor_delete = self.delete(project, neighbor["environment_id"], stopped_neighbor, neighbor_bindings)
        self.check_defaults()
        return {"primary": primary, "primary_workloads": primary_workloads, "neighbor": neighbor, "neighbor_sentinels": sentinels,
                "ready_delete": ready_delete, "recreated": recreated, "original_tombstone_replay": replay,
                "stopped_delete": stopped_delete, "neighbor_delete": neighbor_delete, "sampled_neighbor_liveness": monitor_summary}

    def cleanup(self):
        if self.monitor is not None:
            self.monitor.finish()
            self.monitor = None
        require(not self.unresolved_deletes, "Delete unresolved: automatic Stop/daemon termination withheld; preserve original authority")
        result = super().cleanup()
        result["positive_delete_count"] = len({x["operation"]["operation_id"] for x in self.deleted})
        return result


def run(info):
    os.umask(0o077)
    harness = DeleteHarness(info)
    startup.document(harness.evidence / "inputs.json", info)
    outcome = {"schema_version": 1, "scope": SCOPE, "outcome": "failed", "error": None, "cleanup_errors": [],
        "docker_parity_certified": False, "aggregate_release_certified": False, "test_case_retries": 0,
        "retained_root": str(harness.root), "failure_policy": "no_Delete_retries_preserve_uncertain_operation_and_runtime"}
    try:
        harness.stage()
        outcome["scenario"] = harness.scenario()
        for path, expected in (info["inputs"] | harness.staged_inputs).items():
            require(startup.digest(Path(path)) == expected, "selected input changed during physical run")
    except BaseException as error:
        outcome["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            outcome["cleanup"] = harness.cleanup()
        except BaseException as error:
            outcome["cleanup_errors"].append(f"{type(error).__name__}: {error}")
        try:
            startup.collect_runtime_receipts(harness)
        except BaseException as error:
            outcome["cleanup_errors"].append(f"runtime evidence: {type(error).__name__}: {error}")
        if outcome["error"] is None and not outcome["cleanup_errors"]:
            if outcome["cleanup"]["positive_delete_count"] == 3 and outcome["cleanup"]["daemon_graceful_shutdown_observed"]:
                outcome["outcome"] = "passed_dev_installed_public_delete_happy_path"
            else:
                outcome["cleanup_errors"].append("Delete happy path lacks three completed Environments or positive daemon shutdown")
        outcome["unresolved_delete_requests"] = sorted(harness.unresolved_deletes)
        outcome["unresolved_up_requests"] = sorted(harness.unresolved_up)
        startup.document(harness.evidence / "result.json", outcome)
        startup.checksum_evidence(harness)
    print(json.dumps(outcome), flush=True)
    return 0 if outcome["outcome"].startswith("passed_") else 1


def main(argv):
    try:
        return run(preflight(arguments(argv)))
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
