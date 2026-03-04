# Supply-Chain Security Gates

This document defines release gates for SBOM, provenance, signing, and vulnerability policy.

## Required Artifacts Per Release

For each release artifact `<name>` in `dist/`:

1. SBOM:
- `<name>.sbom.json`

2. Signature:
- `<name>.sig`

3. Provenance attestation:
- `<name>.intoto.jsonl`

4. Vulnerability report:
- `<name>.vulns.json`

## Gate Policy

- Missing SBOM/signature/provenance/vuln report fails the release gate.
- Vulnerability policy:
  - fail on any `critical` vulnerability.
  - fail when `high` count exceeds configured threshold.

## Gate Command

```bash
scripts/check-supply-chain-gates.sh \
  --dist-dir dist \
  --artifact vz \
  --max-high 0
```

Use this check in CI and release promotion jobs.

## Verification Expectations

1. SBOM is present for each shipped binary/image artifact.
2. Signature file exists and is bound to the artifact name.
3. Provenance attestation file exists and is parseable.
4. Vulnerability report is parseable and policy-compliant.
