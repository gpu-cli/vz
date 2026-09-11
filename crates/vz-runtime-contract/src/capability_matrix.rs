//! The checked-in host × target × profile capability matrix, read as truth.
//!
//! `config/host-target-capabilities-v0.4.json` is the source of truth for which
//! Machine capabilities are ACTIVE, DEV, PLANNED or NA on a given host, Machine
//! target and profile. It is validated by
//! `scripts/check-host-target-capabilities.py` and by
//! `crates/vz-runtime-contract/tests/host_target_capabilities.rs`; this module
//! is how the runtime *consumes* it, so that negotiation cannot advertise a
//! capability the matrix does not.
//!
//! Only `status` is read here. Evidence, provenance and the `negotiated_by` /
//! `rejected_by` source citations are inputs to the validators, not to a
//! negotiation decision.
//!
//! The matrix is embedded at compile time, so the answer never depends on a
//! file the daemon happens to find at run time. A matrix that fails to parse
//! fails closed: every capability becomes unadvertised rather than silently
//! granted. [`embedded_matrix_error`] exposes that condition and the unit tests
//! below assert it is absent from the checked-in file.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::Deserialize;

use crate::types::OperatingSystem;
use crate::{Architecture, CapabilitySet, HostSpec, MachineCapability, MachineProfile};

/// The exact bytes the matrix validators check.
const MATRIX_SOURCE: &str = include_str!("../../../config/host-target-capabilities-v0.4.json");

/// Path of the embedded matrix, for diagnostics that must name their source.
pub const MATRIX_PATH: &str = "config/host-target-capabilities-v0.4.json";

/// Delivery status the matrix records for one capability on one pair.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum CapabilityStatus {
    /// Shipped in the published target release with retained installed evidence.
    #[serde(rename = "ACTIVE")]
    Active,
    /// Implemented and demonstrated by an installed slice that is not release certified.
    #[serde(rename = "DEV")]
    Dev,
    /// Committed direction with no negotiation path.
    #[serde(rename = "PLANNED")]
    Planned,
    /// Explicitly rejected by validation as an unsupported pairing.
    #[serde(rename = "NA")]
    NotApplicable,
}

impl CapabilityStatus {
    /// Canonical matrix label.
    pub const fn as_str(self) -> &'static str {
        match self {
            CapabilityStatus::Active => "ACTIVE",
            CapabilityStatus::Dev => "DEV",
            CapabilityStatus::Planned => "PLANNED",
            CapabilityStatus::NotApplicable => "NA",
        }
    }

    /// Whether the runtime may negotiate this capability.
    ///
    /// ACTIVE and DEV both name a real negotiation path; the matrix's own
    /// definitions give PLANNED and NA none, so both are unadvertised.
    pub const fn is_advertised(self) -> bool {
        matches!(self, CapabilityStatus::Active | CapabilityStatus::Dev)
    }
}

impl std::fmt::Display for CapabilityStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One requested capability the matrix does not advertise, with its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnadvertisedCapability {
    /// The capability the caller requested.
    pub capability: MachineCapability,
    /// The status the matrix records for it on this pair.
    pub status: CapabilityStatus,
}

/// The wire key the matrix uses for a host tuple, for example `macos-arm64`.
///
/// The architecture segment is spelled the way the matrix spells it, which is
/// not the serde name of [`Architecture`].
pub fn host_key(host: HostSpec) -> String {
    let os = match host.os {
        OperatingSystem::Linux => "linux",
        OperatingSystem::Macos => "macos",
        OperatingSystem::Windows => "windows",
    };
    let arch = match host.arch {
        Architecture::Aarch64 => "arm64",
        Architecture::X86_64 => "x86_64",
    };
    format!("{os}-{arch}")
}

/// Status the matrix records for one capability on this host × target × profile.
///
/// A pair the matrix does not list, or a capability the pair does not label,
/// takes the matrix's own `unlisted_capability_status`. There is no permissive
/// default: an unknown pair is never a licence to grant.
pub fn capability_status(
    host: HostSpec,
    target: OperatingSystem,
    profile: MachineProfile,
    capability: MachineCapability,
) -> CapabilityStatus {
    let matrix = matrix();
    matrix
        .pairs
        .get(&(host_key(host), target, profile))
        .and_then(|pair| pair.get(&capability).copied())
        .unwrap_or(matrix.unlisted)
}

/// Every capability the matrix advertises for this host × target × profile.
pub fn advertised_capabilities(
    host: HostSpec,
    target: OperatingSystem,
    profile: MachineProfile,
) -> BTreeSet<MachineCapability> {
    let matrix = matrix();
    let Some(pair) = matrix.pairs.get(&(host_key(host), target, profile)) else {
        return BTreeSet::new();
    };
    pair.iter()
        .filter(|(_, status)| status.is_advertised())
        .map(|(capability, _)| *capability)
        .collect()
}

/// The first requested capability this pair does not advertise, if any.
///
/// Capabilities are ordered by [`MachineCapability`], so the answer for a given
/// request is deterministic and a refusal message naming it is reproducible.
pub fn first_unadvertised(
    host: HostSpec,
    target: OperatingSystem,
    profile: MachineProfile,
    requested: &CapabilitySet,
) -> Option<UnadvertisedCapability> {
    requested.capabilities.iter().find_map(|capability| {
        let status = capability_status(host, target, profile, *capability);
        (!status.is_advertised()).then_some(UnadvertisedCapability {
            capability: *capability,
            status,
        })
    })
}

/// Why the embedded matrix could not be read, when it could not be.
///
/// `None` for a well-formed matrix. Any other answer means every capability is
/// unadvertised, which is the fail-closed behavior and never a silent grant.
pub fn embedded_matrix_error() -> Option<&'static str> {
    matrix().parse_error.as_deref()
}

/// Number of host × target × profile pairs the embedded matrix labels.
pub fn embedded_pair_count() -> usize {
    matrix().pairs.len()
}

type PairKey = (String, OperatingSystem, MachineProfile);

struct Matrix {
    unlisted: CapabilityStatus,
    pairs: BTreeMap<PairKey, BTreeMap<MachineCapability, CapabilityStatus>>,
    parse_error: Option<String>,
}

impl Matrix {
    /// Fail closed: no pair is known and nothing unlisted is advertised.
    fn unreadable(reason: String) -> Self {
        Self {
            unlisted: CapabilityStatus::NotApplicable,
            pairs: BTreeMap::new(),
            parse_error: Some(reason),
        }
    }
}

#[derive(Deserialize)]
struct MatrixDocument {
    unlisted_capability_status: CapabilityStatus,
    pairs: Vec<PairDocument>,
}

#[derive(Deserialize)]
struct PairDocument {
    host: String,
    target: OperatingSystem,
    profile: MachineProfile,
    machine_capabilities: BTreeMap<MachineCapability, CapabilityEntryDocument>,
}

#[derive(Deserialize)]
struct CapabilityEntryDocument {
    status: CapabilityStatus,
}

fn matrix() -> &'static Matrix {
    static MATRIX: OnceLock<Matrix> = OnceLock::new();
    MATRIX.get_or_init(
        || match serde_json::from_str::<MatrixDocument>(MATRIX_SOURCE) {
            Ok(document) => {
                let mut pairs = BTreeMap::new();
                for pair in document.pairs {
                    let capabilities = pair
                        .machine_capabilities
                        .into_iter()
                        .map(|(capability, entry)| (capability, entry.status))
                        .collect();
                    if pairs
                        .insert((pair.host.clone(), pair.target, pair.profile), capabilities)
                        .is_some()
                    {
                        return Matrix::unreadable(format!(
                            "{MATRIX_PATH} labels host `{}` twice for the same target and profile",
                            pair.host
                        ));
                    }
                }
                Matrix {
                    unlisted: document.unlisted_capability_status,
                    pairs,
                    parse_error: None,
                }
            }
            Err(error) => Matrix::unreadable(format!("{MATRIX_PATH} is unreadable: {error}")),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MACOS_ARM64: HostSpec = HostSpec {
        os: OperatingSystem::Macos,
        arch: Architecture::Aarch64,
    };

    #[test]
    fn the_checked_in_matrix_parses_and_labels_every_pair() {
        assert_eq!(embedded_matrix_error(), None);
        assert_eq!(embedded_pair_count(), 30);
    }

    /// `as_str` is what refusal `details` carry, so it must be the same name
    /// the matrix and every serialized payload use.
    #[test]
    fn wire_names_agree_with_serde() {
        for capability in [
            MachineCapability::PosixExec,
            MachineCapability::PosixPty,
            MachineCapability::Signals,
            MachineCapability::Files,
            MachineCapability::Ports,
            MachineCapability::DockerEngine,
            MachineCapability::Compose,
            MachineCapability::Buildx,
            MachineCapability::Snapshot,
            MachineCapability::Suspend,
            MachineCapability::Checkpoint,
            MachineCapability::Gui,
            MachineCapability::WindowsConsole,
        ] {
            assert_eq!(
                serde_json::to_value(capability).ok(),
                Some(serde_json::Value::String(capability.as_str().into())),
            );
        }
        for profile in [MachineProfile::Developer, MachineProfile::Hardened] {
            assert_eq!(
                serde_json::to_value(profile).ok(),
                Some(serde_json::Value::String(profile.as_str().into())),
            );
        }
        for os in [
            OperatingSystem::Linux,
            OperatingSystem::Macos,
            OperatingSystem::Windows,
        ] {
            assert_eq!(
                serde_json::to_value(os).ok(),
                Some(serde_json::Value::String(os.as_str().into())),
            );
        }
    }

    #[test]
    fn host_keys_use_the_matrix_spelling_of_each_architecture() {
        assert_eq!(host_key(MACOS_ARM64), "macos-arm64");
        assert_eq!(
            host_key(HostSpec {
                os: OperatingSystem::Linux,
                arch: Architecture::X86_64,
            }),
            "linux-x86_64"
        );
    }

    #[test]
    fn developer_linux_on_apple_silicon_advertises_exactly_its_measured_set() {
        assert_eq!(
            advertised_capabilities(
                MACOS_ARM64,
                OperatingSystem::Linux,
                MachineProfile::Developer
            ),
            BTreeSet::from([
                MachineCapability::PosixExec,
                // Measured at readiness, not assumed: `readiness.rs`
                // runs a PTY probe and negotiates this only when the guest
                // answers on a real terminal.
                MachineCapability::PosixPty,
                MachineCapability::DockerEngine,
                MachineCapability::Compose,
                MachineCapability::Buildx,
            ])
        );
    }

    #[test]
    fn hardened_linux_never_advertises_the_docker_stack() {
        let hardened = advertised_capabilities(
            MACOS_ARM64,
            OperatingSystem::Linux,
            MachineProfile::Hardened,
        );
        // A Hardened Machine gets exec and a terminal -- both measured -- and
        // none of the Docker stack, which is the line this test defends.
        assert_eq!(
            hardened,
            BTreeSet::from([MachineCapability::PosixExec, MachineCapability::PosixPty])
        );
    }

    #[test]
    fn planned_and_not_applicable_capabilities_are_not_advertised() {
        for (capability, expected) in [
            (MachineCapability::Snapshot, CapabilityStatus::Planned),
            (MachineCapability::Suspend, CapabilityStatus::Planned),
            (MachineCapability::Checkpoint, CapabilityStatus::Planned),
            (MachineCapability::Gui, CapabilityStatus::NotApplicable),
            (
                MachineCapability::WindowsConsole,
                CapabilityStatus::NotApplicable,
            ),
        ] {
            let status = capability_status(
                MACOS_ARM64,
                OperatingSystem::Linux,
                MachineProfile::Developer,
                capability,
            );
            assert_eq!(status, expected, "{capability:?}");
            assert!(!status.is_advertised(), "{capability:?}");
        }
    }

    #[test]
    fn an_unlisted_pair_advertises_nothing() {
        let absent = HostSpec {
            os: OperatingSystem::Windows,
            arch: Architecture::Aarch64,
        };
        assert!(
            advertised_capabilities(absent, OperatingSystem::Macos, MachineProfile::Developer)
                .is_empty()
        );
        assert_eq!(
            capability_status(
                absent,
                OperatingSystem::Macos,
                MachineProfile::Developer,
                MachineCapability::PosixExec
            ),
            CapabilityStatus::NotApplicable
        );
    }

    #[test]
    fn first_unadvertised_names_the_lowest_ordered_offender_only() {
        let requested = CapabilitySet::new([
            MachineCapability::PosixExec,
            MachineCapability::Snapshot,
            MachineCapability::Suspend,
        ]);
        assert_eq!(
            first_unadvertised(
                MACOS_ARM64,
                OperatingSystem::Linux,
                MachineProfile::Developer,
                &requested
            ),
            Some(UnadvertisedCapability {
                capability: MachineCapability::Snapshot,
                status: CapabilityStatus::Planned,
            })
        );
        assert_eq!(
            first_unadvertised(
                MACOS_ARM64,
                OperatingSystem::Linux,
                MachineProfile::Developer,
                &CapabilitySet::new([
                    MachineCapability::PosixExec,
                    MachineCapability::DockerEngine,
                ])
            ),
            None
        );
    }
}
