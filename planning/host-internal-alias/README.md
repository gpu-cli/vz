# Superseded 0.3 design: unconditional `host.vz.internal` alias

This plan is retained only as a historical record. It is superseded by the 0.4
Developer Environment network contract in
[`../developer-environments/04-isolation-storage-network.md`](../developer-environments/04-isolation-storage-network.md).

The retired design injected `host.vz.internal` into every stack-managed
container and mapped it to a hardcoded Apple NAT gateway. It also depended on
the host service accepting non-loopback traffic. Those properties made a
network implementation detail act like authorization and could encourage LAN
exposure, so they are not valid 0.4 behavior.

The replacement contract is:

- no host alias or route is injected without a declared import;
- a host import is directional and scoped to one Environment, Machine,
  protocol, and port;
- the destination is an exact host-loopback service reached through a private,
  authenticated relay;
- Internet egress, forwarding, or knowledge of a gateway address does not
  authorize host access; and
- authenticated import relay behavior must not be claimed until its dedicated
  implementation and end-to-end gate land.

The real-VM regression
`undeclared_host_import_does_not_inject_host_vz_internal` protects the
default-deny requirement. It creates a container in a stack network namespace
with no declared import and verifies that `/etc/hosts` has no
`host.vz.internal` entry.
