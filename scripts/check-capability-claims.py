#!/usr/bin/env python3
"""Lint generated-surface capability claims against config/host-target-capabilities-v0.4.json.

Thin wrapper over scripts/helpers/installed_capability_claims.py (stdlib only,
Python 3.9+). Exits nonzero when any surface listed in the matrix's
generated_surfaces carries an unbound or contradicting ACTIVE/DEV/PLANNED/NA
claim, presents a removed CLI root as a command, or defines the status
vocabulary differently from the matrix.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "helpers"))

import installed_capability_claims  # noqa: E402

if __name__ == "__main__":
    sys.exit(installed_capability_claims.main())
