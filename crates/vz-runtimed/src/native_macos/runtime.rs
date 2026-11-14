//! Native VM ownership. Every execution lease retains an exact boot reader.
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};
use vz::{DiskConfig, MacPlatformConfig, Vm, VmConfigBuilder, VmState};
use vz_linux::grpc_client::GrpcAgentClient;
use vz_oci_macos::{DeclaredAttachment, MacosOciError as Error, SharedVmAttachment};
use vz_runtime_contract::{StackRuntimeIdentity, StackRuntimeShutdownRequest};

pub(crate) fn error(value: impl ToString) -> Error {
    Error::InvalidConfig(value.to_string())
}

pub struct NativeMacosRuntime {
    directory: PathBuf,
    cpus: u8,
    memory_mb: u64,
    lifecycle: Arc<RwLock<()>>,
    live: Mutex<Option<Arc<NativeBoot>>>,
}

struct NativeBoot {
    vm: Arc<Vm>,
    identity: StackRuntimeIdentity,
    /// The Environment-network ports this VM was created holding, in NIC order.
    ///
    /// Retained for the same two reasons the Linux backend retains them. A reuse
    /// whose requested ports differ from these is a different Machine
    /// configuration wearing the same name, and a NIC cannot be added to a live
    /// VM; and readiness has to know which address belongs on which link-layer
    /// address, because a macOS guest is told that over the agent channel rather
    /// than on a kernel cmdline it could replay from its own boot.
    attachments: Vec<DeclaredAttachment>,
    /// The egress policy this VM was created under, and its link when that
    /// policy is `allowed`. Retained for the same reason the ports are: a NIC
    /// cannot be added to or removed from a live VM, so a reuse under a
    /// different policy is a different Machine configuration. The link lives
    /// exactly as long as this boot; dropping it ends the translator and every
    /// host connection it opened.
    egress: vz_runtime_contract::EgressPolicy,
    egress_mac: Option<String>,
    egress_link: std::sync::Mutex<Option<super::egress::EgressLink>>,
}

pub struct NativeMacosLease {
    boot: Arc<NativeBoot>,
    _guard: OwnedRwLockReadGuard<()>,
}

/// A start failure retains the exact VM lease so the controller can admit Stop.
#[derive(Debug, thiserror::Error)]
pub enum NativeMacosBootError {
    #[error(transparent)]
    BeforeStart(#[from] Error),
    #[error("native VM start failed: {error}")]
    Start {
        error: Error,
        lease: NativeMacosLease,
    },
}

impl std::fmt::Debug for NativeMacosLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeMacosLease")
            .field("identity", &self.boot.identity)
            .finish_non_exhaustive()
    }
}

impl NativeMacosRuntime {
    pub fn new(directory: PathBuf, cpus: u8, memory_mb: u64) -> Self {
        Self {
            directory,
            cpus,
            memory_mb,
            lifecycle: Arc::new(RwLock::new(())),
            live: Mutex::new(None),
        }
    }

    /// Boot or reuse this Machine's exact native VM.
    ///
    /// `attachments` are the Environment-network ports the fabric plan minted for
    /// this Machine. Each becomes one virtio NIC carrying the MAC the plan
    /// derived: `VmConfigBuilder::build` applies the same NIC resolution to a
    /// macOS boot loader as to a Linux one, and Virtualization.framework attaches
    /// `VZFileHandleNetworkDeviceAttachment` to a macOS guest as readily as to a
    /// Linux one, so the ports a native Machine holds are the ports it declared.
    ///
    /// No NAT NIC is ever added. Besides its declared fabric ports, a Machine
    /// whose declaration says `egress: allowed` holds exactly one more NIC: its
    /// private egress link (`native_macos::egress`). One that says `offline` --
    /// the default -- holds its fabric ports and nothing else, so a native
    /// Machine with no declared network and no egress boots with no network at
    /// all.
    pub async fn boot(
        &self,
        name: &str,
        attachments: Vec<SharedVmAttachment>,
        egress: vz_runtime_contract::EgressPolicy,
    ) -> Result<NativeMacosLease, NativeMacosBootError> {
        let guard = Arc::clone(&self.lifecycle).read_owned().await;
        let mut live = self.live.lock().await;
        if let Some(boot) = live.as_ref() {
            if boot.identity.stack_id != name
                || *boot.vm.state_stream().borrow() != VmState::Running
            {
                return Err(error("native boot is retained but not safely reusable").into());
            }
            let requested: Vec<DeclaredAttachment> = attachments
                .iter()
                .map(|attachment| attachment.declaration().clone())
                .collect();
            if requested != boot.attachments {
                return Err(error(
                    "native boot is retained with different Environment-network ports; a NIC cannot be added to a live VM",
                )
                .into());
            }
            if egress != boot.egress {
                return Err(error(
                    "native boot is retained under a different egress policy; a NIC cannot be added to or removed from a live VM",
                )
                .into());
            }
            // The descriptors go here, exactly as the Linux backend drops them
            // on a matching reuse: this VM already holds the ports it booted
            // with, and a guest end minted for a boot that did not happen must
            // not stay usable. The switch degrades that port and keeps
            // forwarding between the Machines that remain.
            drop(attachments);
            return Ok(NativeMacosLease {
                boot: Arc::clone(boot),
                _guard: guard,
            });
        }
        let (declared, mut nics) = fabric_nics(attachments)?;
        let (egress_mac, egress_link) = match egress_link(name, egress, &declared)? {
            Some((nic, mac, link)) => {
                nics.push(nic);
                (Some(mac), Some(link))
            }
            None => (None, None),
        };
        let config = self.machine_configuration(nics)?;
        let boot = Arc::new(NativeBoot {
            vm: Arc::new(Vm::create(config).await.map_err(error)?),
            identity: StackRuntimeIdentity::new(name).map_err(error)?,
            attachments: declared,
            egress,
            egress_mac,
            egress_link: std::sync::Mutex::new(egress_link),
        });
        // Retain before dispatch: even a failed start is not absence evidence.
        *live = Some(Arc::clone(&boot));
        let result = boot.vm.start().await;
        let lease = NativeMacosLease {
            boot,
            _guard: guard,
        };
        match result {
            Ok(()) => Ok(lease),
            Err(cause) => Err(NativeMacosBootError::Start {
                error: error(cause),
                lease,
            }),
        }
    }

    /// This Machine's VM configuration, given the NICs its ports became.
    ///
    /// Separated from `boot` so the configuration a set of ports produces can be
    /// examined without a registered macOS template to create a VM from.
    fn machine_configuration(&self, nics: Vec<vz::Nic>) -> Result<vz::config::VmConfig, Error> {
        VmConfigBuilder::new()
            .boot_macos()
            .cpus(self.cpus.into())
            .memory_mb(self.memory_mb)
            .disk(DiskConfig {
                id: "system".into(),
                path: self.directory.join("disk.img"),
                read_only: false,
            })
            .mac_platform(MacPlatformConfig {
                hardware_model_path: self.directory.join("hardware-model"),
                auxiliary_storage_path: self.directory.join("auxiliary-storage"),
                machine_identifier_path: self.directory.join("machine-identifier"),
            })
            .nics(nics)
            .enable_vsock()
            .build()
            .map_err(error)
    }

    pub async fn stop_exact(&self, request: &StackRuntimeShutdownRequest) -> Result<(), Error> {
        let _guard = Arc::clone(&self.lifecycle).write_owned().await;
        let mut live = self.live.lock().await;
        let boot = live
            .as_ref()
            .ok_or_else(|| error("native VM absence has no positive stop proof"))?;
        if request.expected != boot.identity {
            return Err(error("native Stop identity mismatch"));
        }
        let mut state = boot.vm.state_stream();
        tracing::info!(state = ?*state.borrow(), "checking exact native VM shutdown state");
        if !boot.vm.has_terminal_state().await.map_err(error)? {
            // The guest owns graceful shutdown. Transport loss is expected.
            // A framework terminal state, followed by releasing this original
            // VM object, is required before publishing positive teardown.
            let shutdown = async {
                let mut client = GrpcAgentClient::connect_default(Arc::clone(&boot.vm)).await?;
                client
                    .exec_stream(
                        "/sbin/shutdown".into(),
                        vec!["-h".into(), "now".into()],
                        Default::default(),
                    )
                    .await?
                    .collect()
                    .await;
                Ok::<_, vz_linux::LinuxError>(())
            };
            let _ = tokio::time::timeout(Duration::from_secs(10), shutdown).await;
            tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    state.borrow_and_update();
                    if boot.vm.has_terminal_state().await.map_err(error)? {
                        return Ok::<(), Error>(());
                    }
                    state.changed().await.map_err(error)?;
                }
            })
            .await
            .map_err(|_| error("native graceful Stop timed out; original VM retained"))??;
        }
        // The VM is terminal, so nothing can use its egress link any more; the
        // translator is joined here so that a completed Stop also means every
        // host connection the Machine had open is closed.
        let link = boot
            .egress_link
            .lock()
            .map_err(|_| error("native egress link lock poisoned"))?
            .take();
        if let Some(mut link) = link {
            link.shutdown().await;
        }
        // Apple's irrecoverable Error state can only be retired by destroying
        // its original VM. The write fence proves no execution lease survives.
        *live = None;
        Ok(())
    }
}

/// One NIC per declared port, in declaration order, each pinned to the address
/// its switch assigned.
///
/// The MAC is not decoration. The switch refuses a frame whose source is not the
/// address it assigned that port, and the guest finds this NIC by matching on the
/// same address, so a NIC that took a fresh random MAC here would come up and
/// then carry nothing in either direction.
fn fabric_nics(
    attachments: Vec<SharedVmAttachment>,
) -> Result<(Vec<DeclaredAttachment>, Vec<vz::Nic>), Error> {
    let mut declared = Vec::with_capacity(attachments.len());
    let mut nics = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let (declaration, socket) = attachment.into_parts();
        let network = vz::FileHandleNetwork::new(socket, declaration.mtu).map_err(error)?;
        nics.push(vz::Nic::file_handle(network).with_mac(declaration.mac.clone()));
        declared.push(declaration);
    }
    Ok((declared, nics))
}

/// The egress NIC and its running link, when the declaration asks for one.
///
/// A declared fabric range that overlaps the link's is refused here, before any
/// VM exists, rather than left for the guest to resolve by interface order.
fn egress_link(
    name: &str,
    egress: vz_runtime_contract::EgressPolicy,
    declared: &[DeclaredAttachment],
) -> Result<Option<(vz::Nic, String, super::egress::EgressLink)>, Error> {
    use super::egress;
    if !egress::link_required(egress) {
        return Ok(None);
    }
    if let Some(conflict) = declared
        .iter()
        .find(|port| egress::overlaps_link(port.ipv4, port.prefix))
    {
        return Err(error(format!(
            "Environment network {} ({}/{}) overlaps the native egress link {}/{}; declare a different cidr",
            conflict.network_id,
            conflict.ipv4,
            conflict.prefix,
            egress::GUEST,
            egress::PREFIX
        )));
    }
    let (guest, link) = egress::EgressLink::open(name).map_err(error)?;
    let network = vz::FileHandleNetwork::new(guest.socket, egress::MTU).map_err(error)?;
    Ok(Some((
        vz::Nic::file_handle(network).with_mac(guest.mac.clone()),
        guest.mac,
        link,
    )))
}

impl NativeMacosLease {
    pub fn identity(&self) -> &StackRuntimeIdentity {
        &self.boot.identity
    }
    /// The link-layer address of this boot's egress NIC, when it has one.
    pub(crate) fn egress_mac(&self) -> Option<&str> {
        self.boot.egress_mac.as_deref()
    }
    /// The Environment-network ports this exact boot holds, in NIC order.
    ///
    /// Readiness addresses them from here rather than from the Up request, so
    /// what it configures is what this VM was actually created with.
    pub(crate) fn attachments(&self) -> &[DeclaredAttachment] {
        &self.boot.attachments
    }
    pub async fn client(&self) -> Result<GrpcAgentClient, Error> {
        if *self.boot.vm.state_stream().borrow() != VmState::Running {
            return Err(error("exact native VM is not running"));
        }
        Ok(GrpcAgentClient::connect_default(Arc::clone(&self.boot.vm)).await?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use std::net::Ipv4Addr;
    use std::os::unix::net::UnixDatagram;

    fn declaration(mac: &str, last_octet: u8) -> DeclaredAttachment {
        DeclaredAttachment {
            network_id: "net_private".into(),
            mac: mac.into(),
            ipv4: Ipv4Addr::new(10, 9, 0, last_octet),
            prefix: 24,
            gateway: None,
            dns: None,
            mtu: 1500,
            hosts: Vec::new(),
        }
    }

    /// One minted port, with a real connected datagram socket as its guest end.
    ///
    /// A real socket rather than a stand-in because every property
    /// `FileHandleNetwork::new` checks — socket type, whether it is connected,
    /// the buffer sizes the kernel grants — is a kernel property that a stand-in
    /// would only assert about itself.
    fn port(mac: &str, last_octet: u8) -> SharedVmAttachment {
        let (host, guest) = UnixDatagram::pair().expect("a connected datagram pair");
        // The host end is deliberately retained for the life of the test:
        // closing it would disconnect the guest end and change what is tested.
        std::mem::forget(host);
        SharedVmAttachment::new(
            declaration(mac, last_octet),
            std::os::fd::OwnedFd::from(guest),
        )
        .expect("a well-formed port declaration")
    }

    fn runtime(directory: &std::path::Path) -> NativeMacosRuntime {
        NativeMacosRuntime::new(directory.to_path_buf(), 4, 8192)
    }

    /// A native macOS Machine's declared ports become real NICs on its VM.
    ///
    /// This is the positive half of lifting the refusal in
    /// `machine_runtime_activation`. "No longer rejected" would be satisfied by a
    /// backend that dropped the ports on the floor, so what is asserted here is
    /// that a macOS boot loader plus one file-handle NIC per declared port is a
    /// configuration that builds, in port order, with each NIC carrying exactly
    /// the address the fabric plan derived for it.
    ///
    /// What this cannot show is a guest: creating the VM needs a registered
    /// macOS template, which this host does not have. The guest-side half that
    /// is testable here lives in `native_macos::fabric`.
    #[test]
    fn a_native_macos_machine_is_configured_with_one_nic_per_declared_port() {
        let directory = tempfile::tempdir().expect("a temporary Machine directory");
        let planned = ["02:aa:bb:cc:dd:03", "02:aa:bb:cc:dd:04"];
        let (declared, nics) = fabric_nics(vec![port(planned[0], 5), port(planned[1], 6)])
            .expect("declared ports become NICs");

        // Declarations and NICs must stay paired: readiness addresses a NIC by
        // the MAC in the declaration that sits at the same position.
        assert_eq!(
            declared.iter().map(|d| d.mac.as_str()).collect::<Vec<_>>(),
            planned
        );
        assert_eq!(nics.len(), 2);
        for nic in &nics {
            // A NAT attachment here would give the Machine egress its
            // declaration never asked for, and would not reach its switch.
            assert!(
                matches!(
                    nic.attachment(),
                    vz::NetworkConfig::FileHandle(network) if network.mtu() == 1500
                ),
                "a fabric port is a switch-backed NIC, not NAT"
            );
        }

        let config = runtime(directory.path())
            .machine_configuration(nics)
            .expect("a macOS boot loader accepts switch-backed NICs");
        // The claim the removed refusal denied: a macOS guest holds these ports,
        // at the addresses the fabric derived, in the order they were declared.
        assert_eq!(config.mac_addresses(), planned);
    }

    /// The egress gate at the VM: `offline` adds no NIC, `allowed` adds exactly
    /// one, switch-free, carrying the Machine's derived egress address and never
    /// a NAT attachment.
    #[tokio::test]
    async fn only_allowed_egress_adds_a_nic_and_it_is_never_nat() {
        use vz_runtime_contract::EgressPolicy;
        assert!(
            egress_link("vm-offline", EgressPolicy::Offline, &[])
                .expect("offline is not an error")
                .is_none()
        );
        let (nic, mac, _link) = egress_link("vm-allowed", EgressPolicy::Allowed, &[])
            .expect("allowed opens a link")
            .expect("and yields a NIC");
        assert_eq!(
            mac,
            super::super::egress::guest_mac("vm-allowed").to_string()
        );
        assert!(
            matches!(nic.attachment(), vz::NetworkConfig::FileHandle(_)),
            "egress is a private link to the daemon, not Apple's shared NAT"
        );
        let directory = tempfile::tempdir().expect("a temporary Machine directory");
        let config = runtime(directory.path())
            .machine_configuration(vec![nic])
            .expect("a macOS boot loader accepts the egress NIC");
        assert_eq!(config.mac_addresses(), [mac]);
    }

    /// A declared network whose range covers the link's would give the guest
    /// two routes for one range, so it is refused before any VM exists.
    #[tokio::test]
    async fn a_fabric_range_overlapping_the_egress_link_is_refused() {
        let mut overlapping = declaration("02:aa:bb:cc:dd:03", 5);
        overlapping.ipv4 = Ipv4Addr::new(198, 18, 0, 5);
        let refused = egress_link(
            "vm-overlap",
            vz_runtime_contract::EgressPolicy::Allowed,
            &[overlapping],
        );
        assert!(refused.is_err());
    }

    /// A native macOS Machine that declares no network still boots with none.
    ///
    /// The Linux backend adds `Nic::nat()` ahead of a Machine's ports when its
    /// default network is enabled. Nothing here does, so lifting the refusal
    /// grants no Machine a link its Environment did not declare — and in
    /// particular does not quietly give native macOS the egress it never had.
    #[test]
    fn a_native_macos_machine_with_no_declared_port_still_has_no_nic_at_all() {
        let directory = tempfile::tempdir().expect("a temporary Machine directory");
        let (declared, nics) = fabric_nics(Vec::new()).expect("no ports is not an error");
        assert!(declared.is_empty());
        let config = runtime(directory.path())
            .machine_configuration(nics)
            .expect("a macOS Machine with no port is still a valid configuration");
        assert!(
            config.mac_addresses().is_empty(),
            "no declared network must mean no NIC, not a default NAT one"
        );
    }
}
