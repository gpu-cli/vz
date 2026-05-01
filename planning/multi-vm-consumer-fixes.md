# Multi-VM Consumer Fixes

> **Status:** Proposed
> **Source:** Virgil consumer feedback (April 2026)
> **Scope:** Three small, generic, upstream-friendly improvements that benefit any
> consumer running more than one concurrent VM in a single process. Surfaced while
> evaluating vz as the macOS hypervisor backend for Virgil's per-user merged-VM +
> Shell-VM + boon-VM topology.

---

## Summary

These are real bugs/gaps in vz that benefit any consumer:

| Change | File | Size | Why |
|---|---|---|---|
| Multi-disk in `VmConfigBuilder` | `crates/vz/src/config.rs:64-66` and `crates/vz/src/bridge.rs:361-385` | ~50 LoC | Today vz takes one `disk_path`; consumers like Virgil need rootfs + data + metadata + bin_override drives. The underlying `setStorageDevices` already accepts arrays. Generic. |
| Source-CID on `VsockListener::accept()` | `crates/vz/src/vsock.rs:317-320` | ~20 LoC | Apple's delegate already receives `fromSocketDevice:`; vz just doesn't propagate it. Required for any consumer that needs source-CID-as-trust-boundary (Virgil's `find_agent_by_cid` is one example). Generic. |
| Per-VM unique MAC (fix fixed-MAC bug) | `crates/vz/src/bridge.rs:401-408` | ~10 LoC | Hard-coded `76:c4:f2:a0:00:01` collides if you run multiple NAT VMs in one process. Replace with `VZMACAddress::randomLocallyAdministratedAddress()` + persist on the `VmConfig` for save/restore. Generic. |

These are small, generic, upstream-friendly, and don't expose any consumer-specific
concepts to the OSS audience.

---

## 1. Multi-disk in `VmConfigBuilder`

### Problem

`VmConfig` exposes a single `disk_path: Option<PathBuf>` field
(`crates/vz/src/config.rs:64-66`). The bridge constructs exactly one
`VZVirtioBlockDeviceConfiguration` and passes it to `setStorageDevices` as a
one-element array (`crates/vz/src/bridge.rs:361-385`).

Consumers running structured microVM workloads typically need a small ordered
set of drives — e.g.:

- `rootfs` (read-write or read-only, primary boot)
- `data` (read-write user/state volume, optionally encrypted)
- `metadata` (read-only configuration drive carrying boot-time tokens)
- `override` (read-only, hot-replaceable for binary update flows)

With the current single-disk surface, consumers either fork the bridge or hack
multiple disks into a single image with a partition table — both bad.

### Proposed change

Replace `disk_path: Option<PathBuf>` with `disks: Vec<DiskConfig>` and add a
`DiskConfig` type:

```rust
#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub id: String,            // stable identifier for logging / future hot-replace
    pub path: PathBuf,
    pub read_only: bool,
}
```

`VmConfigBuilder` gains `.disk(DiskConfig)` (append) alongside the existing
`.disk(PathBuf)` (deprecate-with-shim). The bridge iterates the `Vec` and
constructs one `VZVirtioBlockDeviceConfiguration` per entry, preserving order
(guests see them as `vda`, `vdb`, `vdc`, … in declaration order).

### Underlying support

Apple's `VZVirtualMachineConfiguration::setStorageDevices_` already accepts an
`NSArray<VZStorageDeviceConfiguration *>`. The current single-disk model is a
self-imposed restriction in the wrapper, not a framework limit.

### Backwards compatibility

Keep `disk_path: Option<PathBuf>` as a deprecated convenience that, when set,
appends a single `DiskConfig { id: "rootfs".to_string(), path, read_only: false }`
to `disks`. Remove in vz 0.5.

### Tests

- Unit test in `crates/vz/tests/config_test.rs`: building a `VmConfig` with 3
  disks produces a 3-element `setStorageDevices` array with the right
  read-only flags.
- Integration test in `crates/vz/tests/state_test.rs`: a Linux guest boots with
  rootfs + data, mounts `/dev/vda1` and `/dev/vdb`, writes to both, save/restore
  preserves both.

---

## 2. Source-CID on `VsockListener::accept()`

### Problem

`VsockListener::accept()` returns a `VsockStream` with no information about
which guest CID originated the connection (`crates/vz/src/vsock.rs:317-320`).

Apple's framework already delivers this — the listener delegate's
`listener:shouldAcceptNewConnection:fromSocketDevice:` callback receives a
`VZVirtioSocketDevice *` reference, and the `VZVirtioSocketDevice`'s parent
`VZVirtualMachine` exposes the CID via `VZGenericMachineIdentifier` /
`VZVirtioSocketDeviceConfiguration`. The information is reachable, just not
plumbed through.

Any multi-tenant consumer that uses source CID as a trust boundary
(authenticate-by-where-it-came-from) cannot do so today.

### Proposed change

Extend `VsockStream` (or return a richer accept-result type) to carry the
source CID:

```rust
pub struct AcceptedVsockStream {
    pub stream: VsockStream,
    pub source_cid: u32,
}

impl VsockListener {
    pub async fn accept(&mut self) -> Result<AcceptedVsockStream, VsockError> { ... }
}
```

Or add a method on `VsockStream` (`fn source_cid(&self) -> u32`) that returns
the CID captured at accept time, and keep `accept()` returning `VsockStream`.
Either shape is fine; pick one and document.

### Underlying support

`crates/vz/src/vsock.rs:316-339` already wires the listener delegate. The
`fromSocketDevice:` parameter is currently dropped on the floor at line ~318.
Capturing it and walking up to the owning VM's CID is straightforward.

### Tests

- Integration test that spawns two guests with different CIDs, has each dial
  the same host port, and asserts the host-side accept loop sees the correct
  source CID per accepted stream.

---

## 3. Per-VM unique MAC (fix fixed-MAC bug)

### Problem

Every VM created in a vz process gets the same hard-coded MAC address
`76:c4:f2:a0:00:01` (`crates/vz/src/bridge.rs:401-408`). The constant exists
because `VZVirtioNetworkDeviceConfiguration::new()` randomizes the MAC on
each VM construction, which breaks save/restore (the restored VM's NIC has a
different MAC than the saved state expected, breaking guest-side bridge
membership and DHCP leases).

The fix-once-and-bake-in approach solved save/restore correctness but introduced
a multi-VM correctness bug: two NAT-networked VMs in the same process share a
MAC, which is undefined behavior on any L2 segment. Today this is dormant for
single-VM consumers but lights up the moment a consumer spawns concurrent NAT
VMs.

### Proposed change

Generate a fresh locally-administered MAC per VM and persist it on the
`VmConfig` so save/restore round-trips correctly:

```rust
impl VmConfig {
    pub fn mac_address(&self) -> &VZMACAddress {
        self.network_mac.get_or_init(|| {
            VZMACAddress::randomLocallyAdministratedAddress()
        })
    }
}
```

Where `network_mac` is a `OnceCell<VZMACAddress>` field on `VmConfig` populated
either by:

1. The first `mac_address()` call (fresh random per VM), or
2. Explicit `with_mac(addr)` for consumers that want determinism (e.g., derive
   from `vm_id`).

For save/restore, serialize the MAC alongside the rest of the config.
`VmConfigBuilder` gets a `.mac(VZMACAddress)` setter for the deterministic case.

### Why locally-administered

Locally-administered MAC addresses (LAA, second-least-significant bit of the
first octet set) are guaranteed not to collide with hardware MAC OUIs and are
the appropriate choice for synthetic interfaces.

### Backwards compatibility

Existing single-VM consumers that depend on the constant MAC for any reason
(very unlikely; it's not documented) get a behavior change. Document in the
CHANGELOG. The save/restore correctness story remains intact because the MAC
is now part of the persisted config.

### Tests

- Unit test: two `VmConfig` instances built via the same builder produce
  different MACs.
- Unit test: a `VmConfig` with explicit `with_mac` round-trips through serde.
- Integration test: two NAT-networked Linux guests boot concurrently, each
  gets a distinct MAC visible via `ip link show eth0`, both DHCP successfully
  against the host bridge.

---

## Sequencing

These three changes are independent and can land in any order. Recommended
sequence by ease + risk:

1. **Fixed-MAC fix** (smallest, lowest risk, lights up multi-VM scenarios)
2. **Source-CID on accept** (small, opt-in via the new return type)
3. **Multi-disk** (largest, requires touching the most existing tests)

All three are candidates for a single vz `0.4.0` minor-version release.

---

## Entitlement note

These changes do **not** require any new entitlements. vz today signs and ships
with `com.apple.security.virtualization` enabled in
`entitlements/vz-cli.entitlements.plist`, granted at sign time under the
existing Developer ID. There is no Apple approval gate for this entitlement —
it is included in the entitlements plist and notarization passes without a
multi-week review process. Consumers (including Virgil's Mac client packaging)
inherit the same straightforward signing model.

---

## Out of scope for this doc

The following are **explicitly excluded** because they are consumer-specific or
require deeper Apple framework cooperation:

- Drive hot-replace (no Apple framework API for runtime-mutable
  `VZVirtioBlockDeviceConfiguration`; consumer-specific workarounds belong in
  the consumer's hypervisor adapter, not in vz).
- VirtioFS hot-add/hot-remove (same — no Apple framework API).
- Bridged networking via `VZBridgedNetworkDeviceAttachment` (separate doc;
  requires entitlement work and a different API surface).
- Encrypted snapshot bundle layout (consumer-specific encryption policies do
  not belong in vz; vz writes Apple-encrypted state files and consumers wrap
  them as needed).
- Per-VM network policy enforcement / SOCKS / DNS allowlists (consumer
  concerns, belong in the consumer's host-side relay layer).
