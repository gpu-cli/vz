# SPICE Clipboard Sharing

Enable host-to-guest clipboard sharing so users can Cmd+C on the host and Cmd+V inside the VM.

## Overview

Apple's Virtualization.framework supports clipboard sharing via the SPICE agent protocol. The host-side provides `VZSpiceAgentPortAttachment` attached to a VirtIO console device. The guest must run `spice-vdagent` + `spice-vdagentd` (from UTM's [vd_agent](https://github.com/utmapp/vd_agent) project) to complete the circuit.

**Minimum requirement**: macOS 15 (Sequoia) on both host and guest.

## How It Works

```
Host                                     Guest
┌──────────────────┐                     ┌──────────────────┐
│ NSPasteboard     │                     │ NSPasteboard     │
│ (host clipboard) │                     │ (guest clipboard)│
│       ↕          │                     │       ↕          │
│ VZSpiceAgent-    │  VirtIO console     │  spice-vdagent   │
│ PortAttachment ──┼─────────────────────┼→ spice-vdagentd  │
│ (sharesClipboard │  /dev/tty.com.      │                  │
│  = true)         │  redhat.spice.0     │                  │
└──────────────────┘                     └──────────────────┘
```

1. Host configures a `VZVirtioConsoleDeviceConfiguration` with a `VZSpiceAgentPortAttachment` on port 0
2. Guest runs `spice-vdagentd` (LaunchDaemon) which reads `/dev/tty.com.redhat.spice.0`
3. Guest runs `spice-vdagent` (LaunchAgent) which interacts with the user's pasteboard
4. SPICE protocol handles GRAB/RELEASE/REQUEST/CLIPBOARD messages automatically
5. `VZVirtualMachineView` + `capturesSystemKeys` does NOT interfere — clipboard sync is protocol-level, not keystroke-level

## Success Criteria

- [ ] Cmd+C on host → Cmd+V in VM pastes the host clipboard content
- [ ] Works on first boot (vdagent installed during provisioning)
- [ ] No user intervention required (auto-start daemons)
- [ ] Existing save/restore still works with the console device added

## Phases

### Phase 0: Add SPICE console device to VM config
Depends on: nothing

### Phase 1: Install vdagent in golden image during provisioning
Depends on: Phase 0 (need console device configured for vdagent to connect to)

## Key Decisions

- **vdagent source**: Use UTM's pre-built `spice-vdagent-0.22.1.pkg` (~2 MB) rather than building from source (which requires GLib in both arm64 and x86_64)
- **Installation method**: Download the pkg during `vz provision` and install the binaries into the mounted disk image
- **macOS 15 minimum**: Acceptable — Apple Silicon + Virtualization.framework users are on 15+
- **GPL-3.0 concern**: vd_agent is GPL-3.0 but we're distributing it as a separate binary inside the VM, not linking it into our Rust code. This is fine (same as any Linux distro shipping SPICE tools).
