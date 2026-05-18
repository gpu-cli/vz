# `host.vz.internal` — Sandbox-to-Host DNS Alias

Inject a stable hostname into every stack-managed container that resolves to the macOS host, so workloads can call host-side services without knowing the underlying NAT layout. This is the Docker Desktop equivalent of `host.docker.internal`.

## Why

A sister project (an `AGENTS.jsonc` runtime tool that embeds `vz-sandbox`) needs sandboxed agent CLIs to talk to a host broker daemon over a stable hostname. Today the broker IP is reachable from the guest only as the Apple NAT gateway, which has no name and varies (in principle) per macOS release. Docker users expect `host.docker.internal` to "just work" — we want the same DX.

## What changes

| Layer | Change |
|-------|--------|
| `vz-runtime-contract` | Two new public constants: `HOST_INTERNAL_ALIAS = "host.vz.internal"` and `HOST_INTERNAL_GATEWAY_IPV4 = "192.168.64.1"`. |
| `vz-stack` executor | `prepare_create` pushes `(HOST_INTERNAL_ALIAS, HOST_INTERNAL_GATEWAY_IPV4)` into `RunConfig::extra_hosts` for every service before the existing sibling-service auto-injection. |
| `vz` | `crates/vz/src/protocol.rs` gains a one-line pointer comment noting the constants live in `vz-runtime-contract` (not re-exported to avoid a `vz` ↔ `vz-runtime-contract` dep). |
| Tests | One new unit test verifies single-service stacks still receive the alias; two existing length-asserting tests are updated to acknowledge the extra entry. |
| README | New subsection documenting the alias + the 127.0.0.1 caveat. |

## How it works

```
┌─────────────────────────────┐         ┌────────────────────────────┐
│ macOS host                  │         │ Guest VM (Linux container) │
│                             │         │                            │
│  service on 0.0.0.0:18080   │   NAT   │  curl host.vz.internal     │
│  ─────────────────────────  │ ◄────── │  ─────────────────────────  │
│  Apple bootpd assigns       │         │  /etc/hosts:               │
│   gateway 192.168.64.1      │         │   192.168.64.1 host.vz.    │
│                             │         │                internal    │
└─────────────────────────────┘         └────────────────────────────┘
```

1. Apple's NAT (`VZNATNetworkDeviceAttachment` in `crates/vz/src/bridge.rs:409-441`) gives each guest an IP in `192.168.64.0/24` with `192.168.64.1` as the gateway. The gateway IP **is** the macOS host as seen from inside the guest.
2. `vz-stack` writes `host.vz.internal 192.168.64.1` into each container's `/etc/hosts`, using the same `RunConfig::extra_hosts` plumbing that already handles sibling-service discovery (`crates/vz-oci-macos/src/runtime/bundle.rs:172-191`, post-pivot rewrite at `stack_vm.rs:668-716`).
3. Inside the container, `getent hosts host.vz.internal` returns `192.168.64.1`. A standard TCP/UDP connection from the guest follows Apple's NAT to the host.

## Limitations (documented in README)

1. **127.0.0.1-only host services are unreachable.** The macOS kernel does not route the NAT gateway IP to its own loopback interface. Bind host services to `0.0.0.0` or an explicit non-loopback address (`192.168.64.1` works) to make them reachable via `host.vz.internal`.
2. **Hardcoded `192.168.64.1`.** There is no Virtualization.framework API to query the NAT gateway. If Apple ever changes the default `bootpd` subnet, this constant has to be updated. Reviving `gvproxy` (see open follow-up bead) would replace this with a real DNS server.

## What's intentionally out of scope

- A 127.0.0.1-on-host forwarder. Filed as a separate bead; would either require gvproxy or a userspace TCP proxy bound on `192.168.64.1` that relays to `127.0.0.1` host-side.
- An RPC on the guest agent to discover the NAT gateway dynamically. Considered, but adding an RPC method requires regenerating protobuf and a coordinated guest-agent rollout — disproportionate scope for v1 given the hardcoded IP has been stable across multiple macOS major versions.
- Changes to the inbound port-forwarding code path (the vsock TCP relay in `crates/vz-oci-macos/src/runtime/networking.rs`). That path already works for `127.0.0.1:<host_port>` → container; this work is the complementary outbound direction.

## Verification

- Unit: `cargo nextest run -p vz-stack`. New test `host_vz_internal_injected_into_every_service` plus updated `three_service_ip_allocation` and `default_network_backward_compat`.
- End-to-end (Linux VM): `host_vz_internal_etc_hosts_entry_present_in_container` in `crates/vz-oci-macos/tests/runtime_e2e.rs` boots a real shared VM through the stack pipeline, creates a netns-isolated container with the alias entry, execs `grep host.vz.internal /etc/hosts`, and asserts the row points at `192.168.64.1`. Run with `./scripts/run-sandbox-vm-e2e.sh --suite runtime -- --ignored --nocapture --test-threads=1 --exact host_vz_internal_etc_hosts_entry_present_in_container`.
- A companion `#[ignore]`-gated reachability test (`host_vz_internal_reaches_host_service_via_nat_gateway`) is wired up alongside it. It will start passing once the MASQUERADE follow-up lands.

## Known limitation: netns reachability

DNS injection is complete; **data-plane reachability is not**. Stack-managed containers run in per-service network namespaces created by `vz-guest-agent::network::setup_stack_network`, and the guest agent does not currently program `iptables -t nat -A POSTROUTING -j MASQUERADE` or `net.ipv4.ip_forward=1` for the bridge subnet. Packets from a container reach the bridge gateway but are dropped on the way out of the netns.

Two things are needed to close this:

1. Ship `iptables-legacy` (or `nft`) in the initramfs. The kernel already has `CONFIG_NF_NAT` and `CONFIG_NF_TABLES`; only userspace tooling is missing.
2. Have `setup_stack_network` flip `ip_forward` and add a MASQUERADE rule per bridge.

This is filed as a separate bead so it can be sequenced independently of the DNS-injection work. The injection is still useful on its own — it gives `vz stack`-managed containers a stable, documented hostname for the host, and the reachability fix becomes purely a guest-side patch.

## Future work (tracked separately)

- **vz-0ml** `network: stack-network MASQUERADE for container→host reachability` — the netns reachability gap above. Required to actually make `curl host.vz.internal` work from a stack-managed container.
- **vz-662** `network: revive gvproxy as the production network backend` — would replace hardcoded `192.168.64.1` with a real DNS server, add LAN-bind for inbound forwarding, and add UDP port forwarding. Heavier rewrite.
- `network: 127.0.0.1-on-host reachability for host.vz.internal` — userspace proxy bound on `192.168.64.1` that fans out to `127.0.0.1` host-side. Only worth doing if a concrete user demand surfaces. Not yet filed.
