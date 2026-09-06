"""Validate only the Developer legacy raw-table probe, not Docker parity."""

import argparse
import json
import os
from pathlib import Path
import re
import stat
import sys


class InvalidEvidence(ValueError):
    """The recorded probe does not establish exact raw-table preservation."""


STREAM_LIMIT = 1024 * 1024
DOCUMENT_LIMIT = 8 * 1024 * 1024
ABSENT_RULE = "iptables: Bad rule (does a matching rule exist in that chain?).\n"


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def keys(value, expected, label):
    require(type(value) is dict and set(value) == set(expected), label + " schema mismatch")


def bounded_text(value, limit, label):
    require(type(value) is str, label + " must be text")
    try:
        length = len(value.encode("utf-8"))
    except UnicodeError as error:
        raise InvalidEvidence(label + " is not UTF-8") from error
    require(length <= limit and all(char == "\n" or 32 <= ord(char) < 127 for char in value),
            label + " exceeds bounds or contains noncanonical characters")


def validate(document, version):
    """Validate the new field in an otherwise independently validated document."""
    require(type(version) is str and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version),
            "explicit pinned iptables version required")
    require(type(document) is dict and "legacy_raw_prerouting" in document, "missing raw-table proof")
    value = document["legacy_raw_prerouting"]
    keys(value, {"interface_token", "add_check_delete_and_table_preservation_proven", "probe"}, "raw proof")
    token = value["interface_token"]
    require(type(token) is str and re.fullmatch(r"vzraw[0-9a-f]{10}", token), "invalid owned interface token")
    require(value["add_check_delete_and_table_preservation_proven"] is True, "raw preservation not proved")
    probe = value["probe"]
    keys(probe, {"exit_code", "stdout", "stderr"}, "raw probe")
    require(type(probe["exit_code"]) is int and probe["exit_code"] == 0, "raw probe did not exit successfully")
    bounded_text(probe["stdout"], STREAM_LIMIT, "raw stdout")
    bounded_text(probe["stderr"], 4096, "raw stderr")
    require(probe["stderr"] == ABSENT_RULE * 2, "expected exactly two absent-rule negative controls")
    pattern = (r"iptables_version=iptables v" + re.escape(version) + r" \(legacy\)\n"
               r"raw_before_begin\n(?P<before>.*?)\nraw_before_end\n"
               r"raw_rule_added_and_checked=" + token + r"\n"
               r"raw_after_begin\n(?P<after>.*?)\nraw_after_end\n"
               r"developer-legacy-raw-prerouting-preserved\n")
    match = re.fullmatch(pattern, probe["stdout"], flags=re.DOTALL)
    require(match is not None, "raw markers/version/order differ from the actual probe")
    before, after = match["before"], match["after"]
    require(before == after, "raw table changed during the owned rule probe")
    require(token not in before, "owned interface token preexists in baseline")
    policies = {}
    for line in before.split("\n"):
        require(line.startswith(("-P ", "-N ", "-A ")), "invalid raw table record")
        if line.startswith("-P "):
            policy = re.fullmatch(r"-P (PREROUTING|OUTPUT) (ACCEPT|DROP)", line)
            require(policy is not None and policy[1] not in policies, "invalid or duplicate raw built-in policy")
            policies[policy[1]] = policy[2]
        else:
            require(len(line) > 3 and line.strip() == line, "empty or noncanonical raw table record")
    require(set(policies) == {"PREROUTING", "OUTPUT"}, "both raw built-in policies required")
    return {"interface_token": token, "iptables_version": version, "table_preserved": True}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def reject_constant(value):
    raise InvalidEvidence("nonfinite JSON value: " + value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("filename", type=Path)
    parser.add_argument("--iptables-version", required=True)
    args = parser.parse_args(argv)
    try:
        descriptor = os.open(args.filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            before = os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= DOCUMENT_LIMIT,
                    "bounded single-link regular evidence file required")
            raw = stream.read(DOCUMENT_LIMIT + 1)
            after = os.fstat(stream.fileno())
            require(len(raw) == before.st_size and
                    (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                    (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                    "evidence file changed during bounded read")
        require(len(raw) <= DOCUMENT_LIMIT, "evidence document exceeds bounds")
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object,
                              parse_constant=reject_constant)
        validate(document, args.iptables_version)
    except (OSError, ValueError, RecursionError) as error:
        print("raw-table evidence rejected: " + str(error), file=sys.stderr)
        return 1
    print("Developer legacy raw-table probe validated; not Docker parity certification")
    return 0


if __name__ == "__main__":
    sys.exit(main())
