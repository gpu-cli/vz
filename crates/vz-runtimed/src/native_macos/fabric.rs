//! Guest-side addressing for a native macOS Machine's Environment-network ports.
//!
//! A Linux Machine is addressed by `linux/initramfs/init`, which reads
//! `vz.net.{N}={mac},{ipv4}/{prefix}[,{gw}]` off the kernel cmdline before
//! anything else in the guest runs. A macOS guest has no cmdline hook: Apple's
//! macOS boot loader takes no arguments this host could write, so the same
//! information has to reach the guest some other way.
//!
//! It reaches it over the vsock channel the guest agent already serves, and it
//! is applied by the agent, which runs as a root LaunchDaemon
//! (`vz_macos_provision::guest_agent_launchdaemon_plist`). That is later than
//! the Linux path — the NIC is dark until the agent answers — but it is the only
//! channel that exists before a Machine has an address, and readiness does not
//! publish Ready until this has succeeded.
//!
//! What does *not* change is where the address comes from. There is no DHCP
//! server on a fabric, by design (`environment_switch::plan`: "Addresses are
//! derived, never leased"), because a Machine that stops and comes back must
//! present the address its switch already expects. This module introduces no
//! second source of addresses; it only applies the one the fabric plan derived.
//!
//! The interface is found by MAC and never by name or index. Interface
//! enumeration order inside a guest is not guaranteed, so the host cannot
//! predict which `enN` a fabric NIC becomes, while the MAC is exactly what the
//! host configured the NIC with. This is the rule the Linux side already
//! learned; the reasoning is identical here.

use vz_oci_macos::DeclaredAttachment;

/// Print the single interface whose link-layer address is `want`.
///
/// Ambiguity is refused rather than resolved by position. Two interfaces can
/// legitimately carry one address — a macOS bridge adopts the address of its
/// first member, which `ifconfig -a` on any Mac with `bridge0` shows — and
/// picking the first of them would configure a different interface depending on
/// enumeration order, which is exactly the instability matching by MAC exists to
/// avoid. Zero matches and two matches are both failures, named differently.
///
/// The comparison is textual and case-folding, with each octet re-padded to two
/// digits, because `ether_ntoa` is the one part of this that is a rendering
/// convention rather than a guarantee: BSD lineage has printed both `0:1c:42:…`
/// and `00:1c:42:…` over the years. Normalising both sides costs one loop and
/// removes the question. `strtonum` is deliberately not used: it is a gawk
/// extension and macOS ships the one-true-awk, where it does not exist.
///
/// `want` is folded here as well as by the caller. Folding only the observed
/// side would make this program silently correct for its one caller and wrong
/// for anything else that reached for it, including the test below that feeds it
/// an address in the case some other renderer might use.
const MATCH_INTERFACE_BY_MAC_AWK: &str = r#"
BEGIN { want = tolower(want) }
/^[a-zA-Z]/ { name = $1; sub(/:$/, "", name); next }
$1 == "ether" {
  if (split(tolower($2), o, ":") != 6) next
  for (i = 1; i <= 6; i++) if (length(o[i]) == 1) o[i] = "0" o[i]
  if (o[1] ":" o[2] ":" o[3] ":" o[4] ":" o[5] ":" o[6] != want) next
  found[name] = 1
}
END {
  n = 0
  for (k in found) { n++; only = k }
  if (n == 1) { print only; exit 0 }
  if (n == 0) printf("no interface carries link-layer address %s\n", want) > "/dev/stderr"
  else printf("%d interfaces carry link-layer address %s\n", n, want) > "/dev/stderr"
  exit 1
}
"#;

/// Print `<address> <netmask>` for `want`, read back off the interface.
///
/// The success line is read out of the interface rather than echoed from the
/// arguments, so a run that printed it is evidence the address is on the NIC and
/// not merely evidence that the configuring command exited zero. An interface may
/// hold more than one address — configd hands an unserved link a `169.254/16`
/// self-assigned one — so the filter names the address it is looking for and the
/// `END` rule turns its absence into a non-zero exit rather than empty output.
const OBSERVE_ADDRESS_AWK: &str = r#"
$1 == "inet" && $2 == want { print $2, $4; found = 1 }
END { if (!found) { printf("interface does not hold %s\n", want) > "/dev/stderr"; exit 1 } }
"#;

/// One `sh -c` program that gives a native macOS guest one fabric port's
/// address, and the exact stdout a successful run produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FabricPortConfiguration {
    pub command: String,
    pub args: Vec<String>,
    /// What the guest prints when, and only when, the interface holds the
    /// address afterwards. Compared exactly by the caller.
    pub expected_stdout: String,
}

/// The netmask `ifconfig` is given and the netmask it prints back, for a prefix.
///
/// macOS `ifconfig` takes and renders a mask, never a prefix length, and renders
/// it as `0x%08x`. `SharedVmAttachment::new` has already refused every prefix
/// outside `1..=30`, so the shift below is total.
fn netmask_hex(prefix: u8) -> String {
    let mask = u32::MAX
        .checked_shl(u32::from(32 - prefix.min(32)))
        .unwrap_or(0);
    format!("0x{mask:08x}")
}

/// Render the command that configures one declared port inside the guest.
///
/// Every value interpolated here has already been validated into a shape with no
/// shell metacharacter in it: `SharedVmAttachment::new` refuses a MAC that is not
/// six hex bytes, and the address, prefix and gateway are typed rather than
/// textual. The single quotes are therefore defence in depth rather than the
/// only thing standing between a declaration and the guest's shell.
pub(crate) fn configure_fabric_port(declaration: &DeclaredAttachment) -> FabricPortConfiguration {
    // Lowercased here rather than trusted from the caller, for the same reason
    // `DeclaredAttachment::kernel_argument` lowercases: the guest compares this
    // against a rendering the OS controls, and the fold belongs on the side that
    // can be unit-tested.
    let mac = declaration.mac.to_ascii_lowercase();
    let ipv4 = declaration.ipv4;
    let netmask = netmask_hex(declaration.prefix);
    let route = match declaration.gateway {
        // A fabric gateway is the egress path a `NetworkKind::SimulatedPublic`
        // network will need, so it is a default route rather than an on-link
        // one. Nothing emits a gateway yet, so this line has never run against a
        // guest; when egress lands it also has to settle which NIC owns the
        // default route, and whether replacing an existing default is intended.
        Some(gateway) => format!("/sbin/route -n add -inet default '{gateway}' >/dev/null\n"),
        None => String::new(),
    };
    let program = format!(
        "set -eu\n\
         iface=$(/sbin/ifconfig -a | /usr/bin/awk -v want='{mac}' '{MATCH_INTERFACE_BY_MAC_AWK}')\n\
         /sbin/ifconfig \"$iface\" inet '{ipv4}' netmask '{netmask}' up\n\
         {route}\
         /sbin/ifconfig \"$iface\" inet | /usr/bin/awk -v want='{ipv4}' '{OBSERVE_ADDRESS_AWK}'\n"
    );
    FabricPortConfiguration {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), program],
        expected_stdout: format!("{ipv4} {netmask}\n"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use std::net::Ipv4Addr;

    fn declaration() -> DeclaredAttachment {
        DeclaredAttachment {
            network_id: "net_private".into(),
            mac: "02:AA:BB:CC:DD:03".into(),
            ipv4: Ipv4Addr::new(10, 9, 0, 5),
            prefix: 24,
            gateway: None,
            dns: None,
            mtu: 1500,
            hosts: Vec::new(),
        }
    }

    #[test]
    fn a_prefix_becomes_the_mask_ifconfig_takes_and_prints_back() {
        // The three that matter: the widest and narrowest a port may declare,
        // and the one every derived /24 fabric uses.
        assert_eq!(netmask_hex(24), "0xffffff00");
        assert_eq!(netmask_hex(8), "0xff000000");
        assert_eq!(netmask_hex(30), "0xfffffffc");
    }

    #[test]
    fn the_rendered_command_carries_the_derived_address_and_no_interface_name() {
        let rendered = configure_fabric_port(&declaration());
        assert_eq!(rendered.command, "/bin/sh");
        let program = &rendered.args[1];
        assert!(
            program.contains("want='02:aa:bb:cc:dd:03'"),
            "the MAC is folded to the case the OS renders: {program}"
        );
        assert!(program.contains("inet '10.9.0.5' netmask '0xffffff00' up"));
        assert_eq!(rendered.expected_stdout, "10.9.0.5 0xffffff00\n");
        // The interface is only ever `$iface`, resolved in the guest from the
        // MAC. A literal `en0` here would be the defect this module exists to
        // avoid, and it would pass every other assertion in this file.
        assert!(
            !program.contains("en0"),
            "no interface name may be baked into the command: {program}"
        );
        // No gateway was declared, so no route is installed. A default route the
        // declaration did not ask for would silently move the Machine's egress.
        assert!(!program.contains("route"), "{program}");
    }

    #[test]
    fn a_declared_gateway_becomes_a_default_route_and_its_absence_becomes_none() {
        let mut with_gateway = declaration();
        with_gateway.gateway = Some(Ipv4Addr::new(10, 9, 0, 1));
        let program = configure_fabric_port(&with_gateway).args[1].clone();
        assert!(
            program.contains("/sbin/route -n add -inet default '10.9.0.1'"),
            "{program}"
        );
    }

    /// Run the real MAC-matching program against this host's real `ifconfig -a`.
    ///
    /// The guest is macOS and so is the host, so `ifconfig`'s output format and
    /// `awk`'s dialect are the same on both sides; what this cannot stand in for
    /// is a virtio NIC existing in a guest at all. It is a parser test, run
    /// against output no fixture in this repository wrote, and it reads state
    /// without configuring anything.
    #[test]
    fn the_matching_program_finds_a_real_interface_by_its_real_address() {
        let listing = String::from_utf8(
            std::process::Command::new("/sbin/ifconfig")
                .arg("-a")
                .output()
                .expect("this host runs ifconfig")
                .stdout,
        )
        .expect("ifconfig output is UTF-8");
        // Pick an address that exactly one interface on this host carries, so
        // the ambiguity rule is not what is under test here.
        let mut by_mac: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        let mut name = String::new();
        for line in listing.lines() {
            if line.starts_with(|c: char| c.is_ascii_alphabetic()) {
                name = line.split(':').next().unwrap_or_default().to_string();
            } else if let Some(mac) = line.trim().strip_prefix("ether ") {
                by_mac
                    .entry(mac.trim().to_ascii_lowercase())
                    .or_default()
                    .push(name.clone());
            }
        }
        let (mac, interfaces) = by_mac
            .iter()
            .find(|(_, interfaces)| interfaces.len() == 1)
            .expect("this host has an interface with an unshared link-layer address");

        let matched = run_match(&listing, mac);
        assert_eq!(matched.status.code(), Some(0), "{matched:?}");
        assert_eq!(
            String::from_utf8_lossy(&matched.stdout).trim(),
            interfaces[0],
            "the program named a different interface than the one holding {mac}"
        );
        assert!(matched.stderr.is_empty());

        // An address no interface carries fails loudly rather than printing an
        // empty interface name that the next command would then configure.
        let absent = run_match(&listing, "02:00:00:00:00:ff");
        assert_eq!(absent.status.code(), Some(1), "{absent:?}");
        assert!(absent.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&absent.stderr).contains("no interface carries"),
            "{absent:?}"
        );

        // Uppercase and zero-stripped renderings of the same address resolve to
        // the same interface: this is the half that has to survive whichever
        // `ether_ntoa` the guest's OS build happens to carry.
        let folded = run_match(&listing, &mac.to_ascii_uppercase());
        assert_eq!(
            String::from_utf8_lossy(&folded.stdout).trim(),
            interfaces[0],
            "an uppercase rendering must resolve identically: {folded:?}"
        );
    }

    /// Two interfaces holding one address is refused, not silently resolved.
    ///
    /// Driven from a fixture rather than the host, because it has to be true on
    /// a host whose interfaces happen to have distinct addresses too.
    #[test]
    fn an_address_two_interfaces_share_is_refused_rather_than_guessed() {
        let listing = "en1: flags=8963<UP> mtu 1500\n\tether 36:ad:10:6b:2d:40\n\
                       bridge0: flags=8863<UP> mtu 1500\n\tether 36:ad:10:6b:2d:40\n";
        let ambiguous = run_match(listing, "36:ad:10:6b:2d:40");
        assert_eq!(ambiguous.status.code(), Some(1), "{ambiguous:?}");
        assert!(ambiguous.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&ambiguous.stderr).contains("2 interfaces carry"),
            "{ambiguous:?}"
        );
    }

    /// Run the address read-back program against a real `ifconfig` inet listing.
    ///
    /// `netmask 0x…` and the field positions this depends on are macOS
    /// `ifconfig`'s rendering, not this repository's, so they are read off the
    /// host rather than restated in a fixture.
    #[test]
    fn the_read_back_program_reports_a_real_address_and_its_real_mask() {
        let listing = String::from_utf8(
            std::process::Command::new("/sbin/ifconfig")
                .args(["lo0", "inet"])
                .output()
                .expect("this host runs ifconfig")
                .stdout,
        )
        .expect("ifconfig output is UTF-8");
        let observed = run_awk(&listing, OBSERVE_ADDRESS_AWK, "127.0.0.1");
        assert_eq!(observed.status.code(), Some(0), "{observed:?}");
        assert_eq!(
            String::from_utf8_lossy(&observed.stdout),
            "127.0.0.1 0xff000000\n",
            "loopback's address and mask, as macOS ifconfig renders them"
        );
        // The expectation this module hands the caller is the same string, so
        // the two are checked against one another rather than each restating a
        // literal: a prefix change that broke `netmask_hex` would fail here.
        let expected = format!("{} {}\n", Ipv4Addr::LOCALHOST, netmask_hex(8));
        assert_eq!(String::from_utf8_lossy(&observed.stdout), expected);

        // An address the interface does not hold is a failure, not empty
        // success. Without this rule a NIC that came up unconfigured would read
        // as a clean run that simply printed nothing.
        let missing = run_awk(&listing, OBSERVE_ADDRESS_AWK, "10.9.0.5");
        assert_eq!(missing.status.code(), Some(1), "{missing:?}");
        assert!(missing.stdout.is_empty());
    }

    fn run_match(listing: &str, mac: &str) -> std::process::Output {
        run_awk(listing, MATCH_INTERFACE_BY_MAC_AWK, mac)
    }

    fn run_awk(input: &str, program: &str, want: &str) -> std::process::Output {
        use std::io::Write as _;
        let mut child = std::process::Command::new("/usr/bin/awk")
            .arg("-v")
            .arg(format!("want={want}"))
            .arg(program)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("this host runs awk");
        child
            .stdin
            .take()
            .expect("awk stdin")
            .write_all(input.as_bytes())
            .expect("write the listing to awk");
        child.wait_with_output().expect("awk terminates")
    }
}
