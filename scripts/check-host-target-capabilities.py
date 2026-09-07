#!/usr/bin/env python3
"""Validate config/host-target-capabilities-v0.4.json (stdlib only, Python 3.9+).

Thin wrapper over scripts/helpers/installed_capability_matrix.py. Exits nonzero
on rule violations or evidence digest mismatches; evidence files absent from
this checkout are reported as unverifiable, never as failures.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "helpers"))

import installed_capability_matrix  # noqa: E402

if __name__ == "__main__":
    sys.exit(installed_capability_matrix.main())
