"""Offline fixture-input lint; never runs Docker, a service, or a build."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent
BASE_PIN = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")
IMAGE_PIN = re.compile(r"(?:[^\s@]+@)?sha256:[0-9a-f]{64}")
OWNER = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")


def require(value, message):
    if not value:
        raise ValueError(message)


def validate_inputs(base, image, owner):
    require(BASE_PIN.fullmatch(base or "") is not None, "digest-qualified base required")
    require(IMAGE_PIN.fullmatch(image or "") is not None, "immutable fixture image required")
    require(OWNER.fullmatch(owner or "") is not None, "exact bounded owner token required")


def validate(root=ROOT):
    manifest = json.loads((root / "fixture.json").read_text())
    require(manifest["state"] == "fixture_subset_unverified"
            and manifest["runtime_scenarios_executed"] == 0
            and manifest["compatibility_certified"] is False, "fixture scope overclaim")
    require(manifest["remaining"], "complete fixture coverage not established")
    for name in ("FIXTURE_BASE", "FIXTURE_SSH_BASE"):
        require(manifest["inputs"][name]["resolved_digest"] is None,
                "no base artifact has been pinned in this fixture subset")
    compose = json.loads((root / "compose/compose.json").read_text())
    services = compose["services"]
    require(set(services) == {"db", "api", "worker", "isolated", "failure"}, "service inventory")
    for name, service in services.items():
        require(service["image"] == "${FIXTURE_IMAGE:?immutable fixture image required}"
                and service["pull_policy"] == "never", "mutable fixture image/default")
        require(not {"privileged", "runtime", "container_name", "ports", "network_mode"} & set(service),
                "unscoped service or runtime override")
        require(service["environment"]["FIXTURE_OWNER"] ==
                "${FIXTURE_OWNER:?exact owner token required}", "unbound fixture owner")
        if name != "failure":
            require(service["networks"] == manifest["expected"]["networks"][name], "network drift")
            require(service["healthcheck"]["test"] ==
                    ["CMD", "python3", "/fixture/service.py", "health", name], "health probe drift")
    edges = []
    for name in ("db", "api", "worker", "isolated"):
        for dependency, condition in services[name].get("depends_on", {}).items():
            require(condition == {"condition": "service_healthy"}, "dependency lost healthy gate")
            edges.append([dependency, name])
    require(edges == manifest["expected"]["dependency_edges"], "dependency inventory")
    require(services["db"]["volumes"] == ["state:/data"]
            and compose["volumes"] == {"state": {}}, "volume scope drift")
    require(all(value == {"internal": True} for value in compose["networks"].values()), "network scope")
    require(services["failure"]["profiles"] == ["failure"], "failure entered normal healthy scenario")
    blocked = json.loads((root / "compose/blocked-health.json").read_text())
    require(blocked == {"services": {"db": {"environment": {"FIXTURE_BLOCK_HEALTH": "1"}}}},
            "health-negative overlay changes more than health")

    for path in root.rglob("Dockerfile*"):
        source = path.read_text()
        require(not re.search(r"^#\s*syntax=", source, re.M), "unpinned external Dockerfile frontend")
        require(not re.search(r"^ARG FIXTURE_(?:SSH_)?BASE=", source, re.M), "default base pin")
        first_from = next(line for line in source.splitlines() if line.startswith("FROM "))
        require(first_from.split()[1] in ("${FIXTURE_BASE}", "${FIXTURE_SSH_BASE}"),
                "unbound Dockerfile base")
        require(not re.search(r"^(ADD|COPY) .*inputs", source, re.M), "secret copied into build")
    for path in root.rglob("*.py"):
        ast.parse(path.read_text(), filename=str(path))
    for filename in ("Dockerfile.secret", "Dockerfile.ssh"):
        require("required=true" in (root / "build" / filename).read_text(), "optional credential mount")
    require("type=ssh" not in (root / "build/Dockerfile.ssh-negative").read_text(), "negative SSH has agent")
    require("id=vz04-cache-probe" in (root / "build/Dockerfile.cache").read_text(), "cache ID changed")
    require((root / "build/input.txt").read_bytes() == b"vz04-build-input-v1\n", "build input bytes")
    require((root / "build/ssh-response.txt").read_bytes() ==
            (root / "ssh/response.txt").read_bytes() == manifest["expected"]["ssh_output"].encode(),
            "SSH endpoint expectation drift")
    require((root / "build/.dockerignore").read_text().startswith("*\n"), "unbounded build context")
    for key in ("secret_input", "intermediate_input"):
        require((root / manifest["canaries"][key]).is_file(), "missing canary input")
    expected_hashes = {
        name: hashlib.sha256(value.encode()).hexdigest()
        for name, value in manifest["expected"].items()
        if name in ("build_alpha", "build_beta", "secret_output", "ssh_output")
    }
    require(expected_hashes == manifest["expected_payload_sha256"], "expected payload digest drift")
    secret_hash = hashlib.sha256((root / "inputs/secret.txt").read_bytes()).hexdigest()
    require(secret_hash == manifest["secret_input_sha256"], "secret canary digest drift")
    return {"state": "fixture_subset_inputs_valid", "docker_tests_executed": 0,
            "compatibility_certified": False, "expected_payload_sha256": expected_hashes,
            "secret_input_sha256": secret_hash}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base")
    parser.add_argument("--image")
    parser.add_argument("--owner")
    args = parser.parse_args()
    if any(value is not None for value in (args.base, args.image, args.owner)):
        validate_inputs(args.base, args.image, args.owner)
    print(json.dumps(validate(), sort_keys=True))


if __name__ == "__main__":
    main()
