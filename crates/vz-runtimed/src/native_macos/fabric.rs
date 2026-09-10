//! Guest-side addressing and naming for a native macOS Machine's
//! Environment-network ports.
//!
//! A Linux Machine is addressed by `linux/initramfs/init`, which reads
//! `vz.net.{N}={mac},{ipv4}/{prefix}[,{gw}]` and `vz.host.{N}={ipv4},{name}` off
//! the kernel cmdline before anything else in the guest runs. A macOS guest has
//! no cmdline hook: Apple's macOS boot loader takes no arguments this host could
//! write, so the same information has to reach the guest some other way.
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
//! second source of addresses or names; it only applies the ones the fabric plan
//! derived. A private network's names are a static table for the same reason:
//! nothing can be authoritative about an address that never changes while the
//! Machine lives.
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

/// One `sh -c` program that gives a native macOS guest one piece of its
/// Environment-network configuration, and the exact stdout a successful run
/// produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FabricConfiguration {
    pub command: String,
    pub args: Vec<String>,
    /// What the guest prints when, and only when, the configuration is in place
    /// afterwards, read back out of the guest rather than echoed from the
    /// arguments. Compared exactly by the caller.
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
pub(crate) fn configure_fabric_port(declaration: &DeclaredAttachment) -> FabricConfiguration {
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
    FabricConfiguration {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), program],
        expected_stdout: format!("{ipv4} {netmask}\n"),
    }
}

/// Where a macOS guest resolves a name from a file, before it asks a resolver.
const HOSTS_PATH: &str = "/etc/hosts";

/// The two lines that delimit the block this host owns inside `/etc/hosts`.
///
/// The Linux side truncates the guest's whole `/etc/hosts` and rewrites it from
/// the cmdline, which it can afford because the file it is replacing is one this
/// repository built. A macOS guest's `/etc/hosts` is Apple's, carries entries
/// the OS expects (`broadcasthost`, an IPv6 loopback) and is not ours to
/// discard, so the Environment's names live in a delimited block instead: the
/// block is replaced wholesale on every application and everything outside it is
/// preserved byte for byte.
const HOSTS_BEGIN: &str = "# BEGIN vz Environment endpoints (managed; edits are replaced)";
const HOSTS_END: &str = "# END vz Environment endpoints";

/// Print `/etc/hosts` with this host's block removed, markers included.
///
/// Removal is by exact whole-line match on the two markers rather than by
/// prefix, so a line that merely begins like a marker is data and is preserved.
/// An unterminated block — a guest that lost power midway through a previous
/// write — is consumed to end of file rather than left half-present, because the
/// alternative is a file that grows a new block on every Up while the stale
/// names above it keep resolving.
const STRIP_HOSTS_BLOCK_AWK: &str = r#"
inside { if ($0 == e) inside = 0; next }
$0 == b { inside = 1; next }
{ print }
"#;

/// Print exactly the lines this host's block contains, read back off the file.
///
/// The success line is read out of `/etc/hosts` after the write rather than
/// echoed from the arguments, for the same reason the address is read back off
/// the interface: a `mv` exiting zero says the file was replaced, and only
/// reading the names back says the guest resolves them. A block that is missing
/// entirely prints nothing, which cannot equal a non-empty expectation.
const OBSERVE_HOSTS_BLOCK_AWK: &str = r#"
inside { if ($0 == e) exit; print }
$0 == b { inside = 1 }
"#;

/// Render the command that gives a native macOS guest its Environment's declared
/// endpoint names.
///
/// This is the macOS half of `linux/initramfs/init`'s `vz.host.{N}` handling. It
/// resolves names and nothing more: no listener is bound, no port is probed and
/// nothing waits, so a name landing here is not evidence that anything answers
/// on it.
///
/// The table is the Machine's, not one port's. A Machine writes one `/etc/hosts`
/// however many networks it holds a port on, so the names of every attachment
/// are rendered into one block in attachment order — which is the order the
/// fabric plan minted the ports in, so the file a Machine ends up with depends
/// only on what is persisted.
///
/// A Machine with no declared endpoint still runs this, and gets an empty block.
/// Skipping it would be wrong rather than merely wasteful: a native Machine's
/// disk outlives its boot, so an Environment that has since dropped an endpoint
/// would leave the previous Up's name resolving to an address that now belongs
/// to nothing.
///
/// Every value interpolated here has already been validated into a shape with no
/// shell metacharacter in it — `DeclaredAttachment` refuses an endpoint name that
/// is not ASCII letters, digits, `-`, `.` and `_`, and an address is typed rather
/// than textual — so the single quotes are defence in depth rather than the only
/// thing standing between a declaration and the guest's shell.
pub(crate) fn configure_fabric_hosts(attachments: &[DeclaredAttachment]) -> FabricConfiguration {
    render_fabric_hosts(HOSTS_PATH, attachments)
}

/// `configure_fabric_hosts` against an arbitrary path, so the program itself can
/// be run against a file a test owns rather than the host's own `/etc/hosts`.
fn render_fabric_hosts(path: &str, attachments: &[DeclaredAttachment]) -> FabricConfiguration {
    let entries: String = attachments
        .iter()
        .flat_map(|attachment| attachment.hosts.iter())
        .map(|host| format!(" '{} {}'", host.address, host.name))
        .collect();
    let expected_stdout: String = attachments
        .iter()
        .flat_map(|attachment| attachment.hosts.iter())
        .map(|host| format!("{} {}\n", host.address, host.name))
        .collect();
    // `umask` rather than a `chmod`: the replacement file is created here and
    // `mv` carries its mode, so a guest whose ambient umask hid `/etc/hosts`
    // from every non-root reader would silently stop resolving anything.
    // Written beside the file it replaces so the rename is atomic: a reader that
    // opens `/etc/hosts` at any moment sees the whole previous table or the
    // whole new one, never a partial write.
    let program = format!(
        "set -eu\n\
         umask 022\n\
         /usr/bin/awk -v b='{HOSTS_BEGIN}' -v e='{HOSTS_END}' '{STRIP_HOSTS_BLOCK_AWK}' '{path}' > '{path}.vz'\n\
         printf '%s\\n' '{HOSTS_BEGIN}'{entries} '{HOSTS_END}' >> '{path}.vz'\n\
         /bin/mv '{path}.vz' '{path}'\n\
         /usr/bin/awk -v b='{HOSTS_BEGIN}' -v e='{HOSTS_END}' '{OBSERVE_HOSTS_BLOCK_AWK}' '{path}'\n"
    );
    FabricConfiguration {
        command: "/bin/sh".to_string(),
        args: vec!["-c".to_string(), program],
        expected_stdout,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use std::net::Ipv4Addr;
    use vz_oci_macos::DeclaredHost;

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

    /// A stock macOS `/etc/hosts`, byte for byte, as the clean template ships it.
    const APPLE_HOSTS: &str = "##\n\
                               # Host Database\n\
                               #\n\
                               # localhost is used to configure the loopback interface\n\
                               # when the system is booting.  Do not change this entry.\n\
                               ##\n\
                               127.0.0.1\tlocalhost\n\
                               255.255.255.255\tbroadcasthost\n\
                               ::1             localhost\n";

    fn attachment_with_hosts(hosts: &[(&str, [u8; 4])]) -> DeclaredAttachment {
        DeclaredAttachment {
            hosts: hosts
                .iter()
                .map(|(name, address)| DeclaredHost {
                    name: (*name).to_string(),
                    address: Ipv4Addr::from(*address),
                })
                .collect(),
            ..declaration()
        }
    }

    /// Run a rendered program with `/bin/sh`, the guest's own interpreter.
    fn run_program(rendered: &FabricConfiguration) -> std::process::Output {
        std::process::Command::new(&rendered.command)
            .args(&rendered.args)
            .output()
            .expect("this host runs /bin/sh")
    }

    #[test]
    fn the_rendered_names_are_the_whole_machine_in_attachment_order() {
        let rendered = configure_fabric_hosts(&[
            attachment_with_hosts(&[("api", [10, 9, 0, 5]), ("db", [10, 9, 0, 6])]),
            attachment_with_hosts(&[("cache", [10, 40, 0, 2])]),
        ]);
        assert_eq!(rendered.command, "/bin/sh");
        // One replacement for the whole Machine, not one per port. A second
        // write of this file would REPLACE the first port's block rather than
        // add to it, so a per-port program would leave a Machine on two networks
        // resolving only the names of whichever port ran last.
        assert_eq!(rendered.args[1].matches("/bin/mv").count(), 1);
        assert_eq!(
            rendered.expected_stdout,
            "10.9.0.5 api\n10.9.0.6 db\n10.40.0.2 cache\n"
        );
        // The path is only ever the guest's own `/etc/hosts`.
        assert!(
            rendered.args[1].contains("'/etc/hosts'"),
            "{}",
            rendered.args[1]
        );
    }

    /// The program really writes the block, and really preserves Apple's file.
    ///
    /// Run against a real `/bin/sh` and a real `awk` on a file this test owns,
    /// because every claim here is about what those two programs do rather than
    /// about what this module renders: the guest is macOS and so is this host, so
    /// the interpreter and the awk dialect are the same on both sides.
    #[test]
    fn the_program_writes_the_declared_names_and_keeps_apples_own_entries() {
        let directory = tempfile::tempdir().expect("a temporary guest /etc");
        let hosts = directory.path().join("hosts");
        std::fs::write(&hosts, APPLE_HOSTS).expect("seed a stock macOS hosts file");
        let path = hosts.to_str().expect("a UTF-8 temporary path");

        let rendered = render_fabric_hosts(
            path,
            &[attachment_with_hosts(&[
                ("probe", [10, 88, 215, 209]),
                ("probe-mac", [10, 88, 215, 193]),
            ])],
        );
        let applied = run_program(&rendered);
        assert_eq!(applied.status.code(), Some(0), "{applied:?}");
        assert!(applied.stderr.is_empty(), "{applied:?}");
        // Judged on the read-back, which is the guest's own file and not this
        // program's opinion of what it wrote.
        assert_eq!(
            String::from_utf8_lossy(&applied.stdout),
            rendered.expected_stdout
        );
        let written = std::fs::read_to_string(&hosts).expect("the guest's hosts file");
        assert!(
            written.starts_with(APPLE_HOSTS),
            "Apple's own entries must survive byte for byte: {written}"
        );
        assert!(written.contains("10.88.215.209 probe\n"), "{written}");
        // The replacement is a rename, so no partial file is left behind for a
        // later boot to find.
        assert!(!hosts.with_extension("vz").exists());

        // Applying a CHANGED table replaces the block rather than appending a
        // second one: the dropped name must stop resolving, and the moved one
        // must resolve to its new address and only that.
        let again = render_fabric_hosts(
            path,
            &[attachment_with_hosts(&[("probe", [10, 88, 215, 77])])],
        );
        let reapplied = run_program(&again);
        assert_eq!(reapplied.status.code(), Some(0), "{reapplied:?}");
        assert_eq!(
            String::from_utf8_lossy(&reapplied.stdout),
            "10.88.215.77 probe\n"
        );
        let rewritten = std::fs::read_to_string(&hosts).expect("the guest's hosts file");
        assert!(rewritten.starts_with(APPLE_HOSTS), "{rewritten}");
        assert_eq!(rewritten.matches(HOSTS_BEGIN).count(), 1, "{rewritten}");
        assert!(!rewritten.contains("probe-mac"), "{rewritten}");
        assert!(!rewritten.contains("10.88.215.209"), "{rewritten}");
    }

    /// An Environment that declares no endpoint leaves the guest resolving none.
    ///
    /// The block is emptied rather than left alone. A native Machine's disk
    /// outlives its boot, so a name this host wrote for a previous definition
    /// would otherwise keep resolving to an address that now belongs to nothing.
    #[test]
    fn a_machine_with_no_declared_endpoint_ends_up_with_no_declared_name() {
        let directory = tempfile::tempdir().expect("a temporary guest /etc");
        let hosts = directory.path().join("hosts");
        std::fs::write(&hosts, APPLE_HOSTS).expect("seed a stock macOS hosts file");
        let path = hosts.to_str().expect("a UTF-8 temporary path");

        let seeded = run_program(&render_fabric_hosts(
            path,
            &[attachment_with_hosts(&[("gone", [10, 9, 0, 5])])],
        ));
        assert_eq!(seeded.status.code(), Some(0), "{seeded:?}");

        let cleared = render_fabric_hosts(path, &[attachment_with_hosts(&[])]);
        assert_eq!(cleared.expected_stdout, "");
        let applied = run_program(&cleared);
        assert_eq!(applied.status.code(), Some(0), "{applied:?}");
        assert!(applied.stdout.is_empty(), "{applied:?}");
        let written = std::fs::read_to_string(&hosts).expect("the guest's hosts file");
        assert!(written.starts_with(APPLE_HOSTS), "{written}");
        assert!(!written.contains("gone"), "{written}");

        // A Machine with no port at all is the same claim with nothing to
        // iterate: it must still be a runnable program, not an empty one.
        let none = render_fabric_hosts(path, &[]);
        assert_eq!(none.expected_stdout, "");
        assert_eq!(run_program(&none).status.code(), Some(0));
    }

    /// A block a previous boot never closed is consumed, not left behind.
    ///
    /// Without this the file grows a block per Up while the stale names above the
    /// new one keep resolving, and `/etc/hosts` answers with whichever line a
    /// resolver happened to read first.
    #[test]
    fn an_unterminated_block_from_a_lost_write_is_replaced_rather_than_stacked() {
        let directory = tempfile::tempdir().expect("a temporary guest /etc");
        let hosts = directory.path().join("hosts");
        std::fs::write(
            &hosts,
            format!("{APPLE_HOSTS}{HOSTS_BEGIN}\n10.9.0.5 half-written\n"),
        )
        .expect("seed a torn hosts file");
        let path = hosts.to_str().expect("a UTF-8 temporary path");

        let rendered =
            render_fabric_hosts(path, &[attachment_with_hosts(&[("api", [10, 9, 0, 8])])]);
        let applied = run_program(&rendered);
        assert_eq!(applied.status.code(), Some(0), "{applied:?}");
        assert_eq!(String::from_utf8_lossy(&applied.stdout), "10.9.0.8 api\n");
        let written = std::fs::read_to_string(&hosts).expect("the guest's hosts file");
        assert_eq!(written.matches(HOSTS_BEGIN).count(), 1, "{written}");
        assert!(!written.contains("half-written"), "{written}");
    }

    /// A line that merely looks like a marker is data, and survives.
    #[test]
    fn a_line_that_only_begins_like_a_marker_is_left_alone() {
        let directory = tempfile::tempdir().expect("a temporary guest /etc");
        let hosts = directory.path().join("hosts");
        let lookalike = format!("{HOSTS_BEGIN} but not really\n");
        std::fs::write(&hosts, format!("{APPLE_HOSTS}{lookalike}")).expect("seed a hosts file");
        let path = hosts.to_str().expect("a UTF-8 temporary path");

        let applied = run_program(&render_fabric_hosts(
            path,
            &[attachment_with_hosts(&[("api", [10, 9, 0, 8])])],
        ));
        assert_eq!(applied.status.code(), Some(0), "{applied:?}");
        let written = std::fs::read_to_string(&hosts).expect("the guest's hosts file");
        assert!(written.contains(&lookalike), "{written}");
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
