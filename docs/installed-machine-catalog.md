# Installed Machine catalog (DEV)

Normal `vz up` daemon autostart selects
`<installation-prefix>/machine-target-catalog.json` beside the installed `bin/`
directory. Discovery follows the actual daemon executable, not a project path,
the working directory, a mutable channel, or the legacy `linux/` root alias.
Missing catalogs fail closed. Installing catalog entries does not establish
Machine readiness or certify Docker compatibility.

The installer invokes the installed daemon's offline verifier after downloading
and checking the requested Linux release artifacts. That operation verifies the
actual profile-qualified bundles through the same Rust bundle verifier used by
topology resolution, then atomically publishes a mode-0600 catalog. Only profiles
successfully installed by that transaction are included: an unavailable optional
Hardened download does not adopt a previous release left on disk. A corrupt
bundle prevents publication and leaves the previous catalog bytes unchanged.
`VZ_NO_LINUX=1` does not generate or refresh a catalog.

The existing catalog v1 format binds each `vz-linux-appliance` release and
Developer/Hardened profile to a canonical absolute bundle directory and its
aggregate content digest. Installer-generated entries have no implicit channels.
The project definition must still pin the selected target digest. Installing a
new catalog does not reinterpret the private artifact pins of existing Machines.

An operator can supply `VZ_MACHINE_TARGET_CATALOG=/absolute/catalog.json` for
CLI autostart, or `vz-runtimed --machine-target-catalog /absolute/catalog.json`
when supervising the daemon directly. A present invalid override does not fall
through to installed discovery. The daemon validates catalog ownership, size,
schema, and entries before startup; selected bundles are verified before boot.

Catalog selection is a startup decision. Connecting to an existing daemon does
not reload its catalog, adopt its Machines, or replace its socket. An existing
empty or incompatible catalog can therefore report that the exact target is
unavailable. Version/protocol mismatch also fails without terminating the daemon.
Any intentional daemon replacement must first account for its owned Environments
and use the process supervision mechanism that started that exact daemon.

Crash recovery is not yet implemented for this DEV path. A daemon killed before
graceful socket cleanup leaves a socket that blocks normal Up autostart, even if
its Machines had already stopped. Removing that path alone would not recover
authoritative ownership of previously active Machines. Neither a PID file nor a
failed connection authorizes deletion or adoption. Durable daemon/socket claims
and backend-aware recovery remain required for 0.4; graceful Stop/Up evidence
does not certify daemon-crash recovery.

The internal installer mode is not a public `vz` lifecycle verb:

```sh
/absolute/prefix/bin/vz-runtimed \
  --write-installed-machine-target-catalog /absolute/prefix \
  --installed-release-version 0.4.0 \
  --installed-linux-profile developer \
  --installed-linux-profile container
```

It has a 120-second verification deadline and starts no daemon, VM, or Docker
client. It requires the specified profiles to have already been installed;
the invocation itself does not download or repair artifacts.
