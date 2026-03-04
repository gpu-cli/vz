# Build Sweep Context Mapping

`vz validate sweep-build` executes external Dockerfile compatibility cases with
explicit per-case build context metadata.

This prevents false negatives where a Dockerfile lives in a subdirectory but
expects repository-root context (for example `COPY binder/README.ipynb ...`).

## Manifest schema

```json
{
  "cases": [
    {
      "id": "binder-root-context",
      "dockerfile": "docker-stacks/binder/Dockerfile",
      "context": ".",
      "tag": "vz-sweep:binder",
      "build_args": {
        "FOO": "bar"
      }
    }
  ]
}
```

Fields:

- `id` (required): case identifier.
- `dockerfile` (required): Dockerfile path (relative to `repo_root` unless absolute).
- `context` (optional): context directory for this case.
  - If omitted, defaults to Dockerfile parent directory (legacy behavior).
- `repo_root` (optional): per-case root override.
- `tag` (optional): output image tag override.
- `build_args` (optional): map of build args.
- `target` (optional): multi-stage target.

## CLI

```bash
vz validate sweep-build \
  --manifest ./compat-build-cases.json \
  --repo-root /path/to/repo \
  --json
```

Dry-run (path resolution only):

```bash
vz validate sweep-build --manifest ./compat-build-cases.json --dry-run --json
```
