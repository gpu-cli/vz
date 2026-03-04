# Independent Security Assessment Gate

This document defines the GA gate for third-party security assessment and remediation closure.

## Required Artifacts

Store under `.artifacts/security-assessment/`:

1. `scope.md`
- assessment scope, in-scope systems, test window, assessor identity.

2. `report.md`
- final external assessment report.

3. `findings.json`
- machine-readable findings with fields:
  - `id`
  - `severity` (`critical|high|medium|low`)
  - `status` (`open|mitigated|accepted`)
  - `owner`
  - `evidence`

4. `regression-tests.md`
- tests added for remediated classes.

## Gate Policy

- Fail release on any `critical` finding with status `open`.
- Fail release on any `high` finding with status `open` unless explicitly risk-accepted.
- Require regression test linkage for each `mitigated` critical/high finding.

## Gate Command

```bash
scripts/check-security-assessment-gate.sh --assessment-dir .artifacts/security-assessment
```

## Closure Criteria

1. External report complete and archived.
2. Critical/high findings resolved or risk-accepted with explicit sign-off.
3. Regression coverage documented for mitigated findings.
