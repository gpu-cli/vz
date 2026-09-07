"""Read-only platform ownership observer for the installed native macOS gate."""
import argparse
import hashlib
import json
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-machines", type=int, default=2)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    layout = json.loads((evidence / "layout.json").read_text())
    stores = Path(layout["root"]) / "r/topology-machines"
    observations = []
    previous = None
    while not (evidence / "summary.json").exists():
        rows = {}
        for identifier in stores.glob("*/data/native-target/machine-identifier"):
            directory = identifier.parent
            try:
                rows[str(directory)] = {
                    "identifier_sha256": hashlib.sha256(identifier.read_bytes()).hexdigest(),
                    "files": {
                        name: {"inode": (directory / name).stat().st_ino,
                               "mode": oct((directory / name).stat().st_mode & 0o777),
                               "links": (directory / name).stat().st_nlink}
                        for name in ["disk.img", "auxiliary-storage", "machine-identifier"]
                    },
                }
            except FileNotFoundError:
                continue  # Concurrent positive Delete may retire a directory.
        if rows != previous:
            observations.append(dict(time=time.time(), machines=rows))
            previous = rows
            (evidence / "platform-observations.json").write_text(json.dumps(observations, indent=2))
        time.sleep(0.2)
    assert json.loads((evidence / "summary.json").read_text())["passed"], "lifecycle gate failed"
    machines = {key: value for row in observations for key, value in row["machines"].items()}
    assert len(machines) == args.expected_machines, machines
    assert len({row["identifier_sha256"] for row in machines.values()}) == args.expected_machines
    for key in machines:
        assert len({row["machines"][key]["identifier_sha256"] for row in observations
                    if key in row["machines"]}) == 1, "platform identity changed across restart"
    for name in ["disk.img", "auxiliary-storage", "machine-identifier"]:
        assert len({row["files"][name]["inode"] for row in machines.values()}) == args.expected_machines
        assert all(row["files"][name]["links"] == 1 and
                   int(row["files"][name]["mode"], 8) & 0o077 == 0 for row in machines.values())
    (evidence / "platform-audit.json").write_text(json.dumps(dict(
        passed=True, machines=machines,
        scope="private platform identities across installed Stop/Up and isolated Environments"), indent=2))


if __name__ == "__main__":
    main()
