//! Destinations this guest configured and is willing to relay to.
//!
//! `PortForward` must never dial an address chosen by its caller: the product
//! contract requires exact authenticated grants to a declared service, and a
//! guest that honours a caller-supplied host turns the relay into an arbitrary
//! outbound connector the moment it carries host imports or exports.
//!
//! So the guest keeps the address side of the relay itself. Setting up a
//! service network namespace records one grant — the primary address the guest
//! assigned to that service. The open frame names a grant, and a name the guest
//! never configured has no address and is refused.
//!
//! Service names are already unique across a guest: `setup_stack_network`
//! creates one named namespace per service under `/var/run/netns`, so a second
//! service of the same name cannot exist. `grant` enforces the same uniqueness
//! for addresses instead of silently re-pointing an existing grant.

// Every guest resolves grants on the relay path, but only a Linux guest builds
// the service networks that record them: a native macOS guest agent has no
// service networking, so for it the registry is permanently empty and the
// recording half of this module is never reached.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::collections::BTreeMap;
use std::fmt;
use std::net::Ipv4Addr;
use std::sync::{Mutex, OnceLock, PoisonError};

/// A grant could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GrantError {
    /// The service already has a different address. Re-pointing a live grant
    /// would silently redirect any relay already using it.
    Conflict {
        service: String,
        existing: Ipv4Addr,
        proposed: Ipv4Addr,
    },
}

impl fmt::Display for GrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict {
                service,
                existing,
                proposed,
            } => write!(
                f,
                "service '{service}' is already forwarded to {existing}; refusing to re-point it to {proposed}"
            ),
        }
    }
}

impl std::error::Error for GrantError {}

/// The set of services this guest will relay to, by the address it assigned.
#[derive(Debug, Default)]
pub(crate) struct ForwardGrants {
    grants: Mutex<BTreeMap<String, Ipv4Addr>>,
}

impl ForwardGrants {
    pub(crate) const fn new() -> Self {
        Self {
            grants: Mutex::new(BTreeMap::new()),
        }
    }

    /// Record the primary address assigned to `service`.
    ///
    /// Recording the same address twice is the idempotent retry of a setup and
    /// succeeds; a different address for a live name is refused.
    pub(crate) fn grant(&self, service: &str, address: Ipv4Addr) -> Result<(), GrantError> {
        let mut grants = self.grants.lock().unwrap_or_else(PoisonError::into_inner);
        match grants.get(service) {
            Some(&existing) if existing == address => Ok(()),
            Some(&existing) => Err(GrantError::Conflict {
                service: service.to_string(),
                existing,
                proposed: address,
            }),
            None => {
                grants.insert(service.to_string(), address);
                Ok(())
            }
        }
    }

    /// Drop the grants for `services`, which are being torn down.
    pub(crate) fn revoke(&self, services: &[String]) {
        let mut grants = self.grants.lock().unwrap_or_else(PoisonError::into_inner);
        for service in services {
            grants.remove(service);
        }
    }

    /// The address of a granted service, or `None` if it was never configured.
    pub(crate) fn resolve(&self, service: &str) -> Option<Ipv4Addr> {
        self.grants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(service)
            .copied()
    }

    /// Decide where a `PortForwardOpen` may connect.
    ///
    /// An empty name is the guest's own loopback — the fixed default, not a
    /// choice the caller made. Every other name must be a service this guest
    /// configured. Nothing here parses the name as an address, so a caller that
    /// sends one is refused rather than obeyed.
    pub(crate) fn destination(&self, target_service: &str) -> Result<Ipv4Addr, NoGrant> {
        if target_service.is_empty() {
            return Ok(Ipv4Addr::LOCALHOST);
        }
        self.resolve(target_service).ok_or_else(|| NoGrant {
            service: target_service.to_string(),
        })
    }
}

/// The named service has no forwarding grant on this guest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NoGrant {
    pub(crate) service: String,
}

impl fmt::Display for NoGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no forwarding grant for service '{}'; this guest configured no such service",
            self.service
        )
    }
}

impl std::error::Error for NoGrant {}

/// The process-wide grants. Network setup and the relay live on separate gRPC
/// services with no shared handle, and both act on the one guest they run in.
pub(crate) fn grants() -> &'static ForwardGrants {
    static GRANTS: OnceLock<ForwardGrants> = OnceLock::new();
    GRANTS.get_or_init(ForwardGrants::new)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    #[test]
    fn an_unconfigured_name_has_no_address() {
        let grants = ForwardGrants::new();
        assert_eq!(grants.resolve("db"), None);
        // An address is a name the guest never configured, not a destination.
        assert_eq!(grants.resolve("172.20.0.2"), None);
        assert_eq!(grants.resolve("127.0.0.1"), None);
    }

    #[test]
    fn a_granted_service_resolves_to_the_address_the_guest_assigned() {
        let grants = ForwardGrants::new();
        grants
            .grant("db", Ipv4Addr::new(172, 20, 0, 3))
            .expect("first grant");
        assert_eq!(grants.resolve("db"), Some(Ipv4Addr::new(172, 20, 0, 3)));
    }

    #[test]
    fn re_recording_the_same_address_is_an_idempotent_setup_retry() {
        let grants = ForwardGrants::new();
        let addr = Ipv4Addr::new(172, 20, 0, 3);
        grants.grant("db", addr).expect("first grant");
        grants.grant("db", addr).expect("retry");
        assert_eq!(grants.resolve("db"), Some(addr));
    }

    #[test]
    fn re_pointing_a_live_grant_is_refused() {
        let grants = ForwardGrants::new();
        grants
            .grant("db", Ipv4Addr::new(172, 20, 0, 3))
            .expect("first grant");
        let error = grants
            .grant("db", Ipv4Addr::new(10, 0, 0, 9))
            .expect_err("second address for a live name");
        assert_eq!(
            error,
            GrantError::Conflict {
                service: "db".to_string(),
                existing: Ipv4Addr::new(172, 20, 0, 3),
                proposed: Ipv4Addr::new(10, 0, 0, 9),
            }
        );
        assert_eq!(grants.resolve("db"), Some(Ipv4Addr::new(172, 20, 0, 3)));
    }

    #[test]
    fn an_empty_name_is_the_guests_own_loopback() {
        let grants = ForwardGrants::new();
        assert_eq!(grants.destination(""), Ok(Ipv4Addr::LOCALHOST));
    }

    #[test]
    fn a_caller_supplied_address_is_refused_not_dialled() {
        let grants = ForwardGrants::new();
        grants
            .grant("db", Ipv4Addr::new(172, 20, 0, 3))
            .expect("db grant");
        // The exact shape the old wire format honoured: a destination chosen by
        // the caller. It is a name with no grant, so it is refused.
        for chosen in ["172.20.0.3", "10.0.0.1", "127.0.0.1", "169.254.169.254"] {
            assert_eq!(
                grants.destination(chosen),
                Err(NoGrant {
                    service: chosen.to_string()
                }),
                "{chosen} must not resolve"
            );
        }
        // The grant it could have been spoofing still resolves normally.
        assert_eq!(grants.destination("db"), Ok(Ipv4Addr::new(172, 20, 0, 3)));
    }

    #[test]
    fn a_revoked_service_is_refused_rather_than_dialled_at_its_old_address() {
        let grants = ForwardGrants::new();
        grants
            .grant("db", Ipv4Addr::new(172, 20, 0, 3))
            .expect("db grant");
        grants.revoke(&["db".to_string()]);
        assert_eq!(
            grants.destination("db"),
            Err(NoGrant {
                service: "db".to_string()
            })
        );
    }

    #[test]
    fn revoking_a_service_removes_its_destination_and_leaves_the_others() {
        let grants = ForwardGrants::new();
        grants
            .grant("db", Ipv4Addr::new(172, 20, 0, 3))
            .expect("db grant");
        grants
            .grant("web", Ipv4Addr::new(172, 20, 0, 2))
            .expect("web grant");
        grants.revoke(&["db".to_string()]);
        assert_eq!(grants.resolve("db"), None);
        assert_eq!(grants.resolve("web"), Some(Ipv4Addr::new(172, 20, 0, 2)));
        // A revoked name may be re-granted at a new address by the next setup.
        grants
            .grant("db", Ipv4Addr::new(10, 0, 0, 9))
            .expect("re-grant after revoke");
        assert_eq!(grants.resolve("db"), Some(Ipv4Addr::new(10, 0, 0, 9)));
    }
}
