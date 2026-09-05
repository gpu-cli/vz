# Developer guest public CA trust

`cacert.pem` and `LICENSE` are byte-for-byte members of the immutable certifi
2026.7.22 wheel. `inputs.json` records the exact official PyPI release metadata,
content-addressed wheel URL/SHA-256, member paths, hashes and certificate count.
Only the public Mozilla root collection is included; neither Mac keychains nor
the build container's dynamic trust store are inputs.

Normal Developer initramfs assembly verifies these vendored bytes offline and
installs the bundle at the reserved control-plane path `/etc/vz/ca-certificates.crt`.
The init script verifies the pinned digest and
propagates the same bytes into the actual overlay root before starting the guest
agent. Developer init exports `SSL_CERT_FILE=/etc/vz/ca-certificates.crt`
and `SSL_CERT_DIR=/etc/vz/empty-ca-directory`; the latter is verified empty so Go
cannot append roots from other conventional guest trust directories.
Image-provided `/etc/ssl` paths, CA bytes and symlinks are neither inspected nor
changed; distro trust remains separate from the selected control-plane trust.
Missing, modified or redirected CA files fail Developer startup; they do
not enable TLS bypass or trigger fallback to another rootfs. Hardened receives
none of the CA payload/helper/license/provenance files.

The complete payload is transitively bound by `version.json`'s initramfs SHA-256
and the normal installed Machine artifact identity. There is no additional loose
artifact or runtime download. Updating trust is an explicit source/pin update:
verify an exact upstream wheel hash, retain its public bundle and license without
modification, update provenance, then rebuild and physically verify a new bundle.

Offline checks:

```
python3 linux/ca-trust.py --source "$PWD/linux/ca-trust"
python3 -m unittest discover -s linux -p test_ca_trust.py
```

These tests do not certify registry TLS. The physical gate must pull the exact
pinned registry manifest through the actual per-Machine managed Docker context,
verify image identity and execute its payload. No successful network claim is
made merely because a CA file is present.
