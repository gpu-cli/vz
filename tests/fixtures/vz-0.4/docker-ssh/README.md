# Owned BuildKit SSH fixture (DEV)

This sibling fixture does not modify the existing Docker or parallel fixtures.
It is a protocol/recipe input, not standalone release certification. Host
orchestration must authenticate the exact Machine, Engine, bridge address,
builder, disposable agent, server host key and package/base inputs.

Stage the eight authenticated offline Debian OpenSSH DEBs at `packages/` with
`packages/manifest.json` equal to the checked-in `package-pins.json`. Server and
clients use the same registry-resolvable pinned Python ARM64 base. A network-none
RUN verifies every DEB and the base-owned `dpkg-deb`, GNU tar and ELF loader
before extracting data only. Each `dpkg-deb --fsys-tarfile` capture is limited to
16 MiB stdout, 8 KiB stderr and 15 seconds, then spooled into an owned private
guest directory. Explicit GNU tar `--keep-directory-symlink` preserves the
authenticated merged-/usr aliases; their root ownership, targets and identities,
and the pinned tool/loader bytes are checked before and after every extraction.
`dpkg-deb --extract` cannot be used here: it unsets `TAR_OPTIONS` and its default
tar invocation replaces `/lib` with a directory, hiding the ELF loader.
No inherited `TAR_OPTIONS`, mutable apt operation or maintainer script runs.
SSH subprocess limits and sanitized environment are unchanged. If process
reaping is unproven, its private spool is retained for Machine lifecycle cleanup.
`server.py`
installs fresh dedicated `vzssh` (10001) and privilege-separation `sshd` (10002)
accounts, rejecting any existing name/ID instead of repairing or unlocking it.
The server image copies only its public Python modules and sshd configuration
with explicit `0644` modes. Preparation and startup require those files to be
single-link, root-owned regular files in root-owned `0755` `/fixture`; the
unprivileged forced command can read them without changing host staging modes
or runtime private-key permissions.
The Linux OpenSSH build treats shadow `!` as locked; shadow `*` is not a usable
password and is not that prefix. The fixture uses `*` with no expiry fields and
also disables PAM/password/interactive authentication. Source evidence is the
authenticated Debian 9.2p1-2+deb12u10 `configure.ac`, `platform.c`, `auth.c`,
`sshd.c` and Debian rules/postinst. Root and all existing account rows are
preserved. Real image/server behavior still requires the installed-Mac lane.

Public per-solve input is exactly `inputs/request.json` and
`inputs/known_hosts`. The request fields are `schema_version:1`, a disposable
`token` matching `vzssh-[0-9a-f]{24}`, the inspected private IPv4 `host`,
`port:2222`, and the actual server's Ed25519 `host_key_fingerprint`.
The known-host file is one canonical `[IPv4]:2222 ssh-ed25519 BASE64` line.
Neither file contains private keys. Keep all authentication/host private keys,
agent sockets and authorized-key/server runtime files outside build contexts.

`Dockerfile.ssh` is shared by declared success, wrong-known-host, and omitted
provider recipes. Only the pinned public known-host bytes change for the
wrong-host case. Omitting `--ssh fixture=<owned-socket>` must fail before the
required-mount RUN executes; accepting a probe acknowledgment is incorrect.
`Dockerfile.undeclared` has no SSH mount even when the host supplies that same
provider. All four recipes must be fresh uncached solves.

The probe emits bounded public JSON. Exact authentication success returns 0;
the exact OpenSSH public-key denial returns 41; an exact changed-host-key
diagnostic bound to the actual server fingerprint returns 42. Every other
diagnostic/status combination, timeout, oversized output, malformed input or
missing mount returns operational failure 70, never an accepted denial.
Arbitrary SSH stdout/stderr and exception strings are not printed. Public
result hashes and sizes permit reconstruction of the exact admitted diagnostic
from public request fields and the source-pinned grammar. These grammars follow
OpenSSH 9.2p1 `sshconnect.c`, `sshconnect2.c`, `log.c`, and Debian's
`mention-ssh-keygen-on-keychange.patch` (including its two extra warning lines).
Each logger record ends CRLF, while the fingerprint's embedded newline is LF.
Physical evidence must independently confirm the complete selected grammar.

The success recipe's next RUN proves both the declared agent path and agent
environment are absent. Its scratch OCI output contains only `ssh.txt`, uid/gid
0, mode 0644, with exact `vz-ssh-response:<token>\n` bytes. Host replay must bind
both RUNs, validate that OCI graph and inspect image/cache/log streams for key
leakage. The server exposes only its Machine-local Docker bridge port 2222;
there is no host-published port or SSH host import. Its forced response command
rejects any alternate original command; password login, root login, forwarding,
PTY and user RC/environment are disabled.

The orchestrator injects exact-owned server runtime files under
`/run/vz-ssh-server`: `host_key` root:0600, `authorized_keys` root:0444, and
`response.txt` root:0444, directory root:0755. They are runtime-only inputs,
not image layers. Stop/delete ordering, agent reaping, key/socket removal,
public Machine Stop and checksum-bound evidence remain harness responsibilities.

Offline tests (no network or VM):

```sh
PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -m unittest discover -s tests/fixtures/vz-0.4/docker-ssh -p 'test_*.py'
```
