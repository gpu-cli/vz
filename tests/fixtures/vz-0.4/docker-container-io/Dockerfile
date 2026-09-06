ARG FIXTURE_BASE=docker.io/library/python@sha256:d04f49f5882f49a3b91f874e75e19f0c265f7222da8659741a9d7eab148f22a9
FROM ${FIXTURE_BASE}
COPY --chmod=0644 probe.py /fixture/probe.py
RUN --network=none python3 -c "import os,pathlib; p=pathlib.Path('/fixture'); assert p.is_dir() and not p.is_symlink(); os.chmod(p,0o755); pathlib.Path('/workspace').mkdir(mode=0o755); p=pathlib.Path('/vz-container-io-root.txt'); p.write_bytes(b'vz-container-io-root-v1\n'); os.chmod(p,0o644); p=pathlib.Path('/fixture/not-executable'); p.write_bytes(b'#!/bin/sh\nexit 0\n'); os.chmod(p,0o644)"
WORKDIR /workspace
ENTRYPOINT ["python3", "-u", "/fixture/probe.py"]
