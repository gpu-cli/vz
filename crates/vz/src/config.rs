//! VM configuration builder.

use std::path::PathBuf;

use crate::VzError;

/// How to boot the VM.
#[derive(Debug, Clone)]
pub enum BootLoader {
    /// Boot macOS from a disk image.
    ///
    /// Requires `MacPlatformConfig` to be set via `VmConfigBuilder::mac_platform`.
    MacOS,
    /// Boot Linux with a kernel, optional initrd, and command line.
    Linux {
        kernel: PathBuf,
        initrd: Option<PathBuf>,
        cmdline: String,
    },
}

/// macOS platform configuration for Apple Silicon VMs.
///
/// These files are generated during `install_macos` and must be preserved
/// across VM restarts. They identify the virtual hardware to the guest OS.
#[derive(Debug, Clone)]
pub struct MacPlatformConfig {
    /// Path to the hardware model data file.
    /// Created during macOS installation from the IPSW restore image.
    pub hardware_model_path: PathBuf,
    /// Path to the machine identifier data file.
    /// A unique identifier for this VM instance.
    pub machine_identifier_path: PathBuf,
    /// Path to the auxiliary storage file (NVRAM equivalent).
    /// Contains boot configuration and OS settings.
    pub auxiliary_storage_path: PathBuf,
}

/// A directory shared between host and guest via VirtioFS.
#[derive(Debug, Clone)]
pub struct SharedDirConfig {
    /// Tag the guest uses to mount this share (e.g., `mount -t virtiofs <tag> /mnt/project`).
    pub tag: String,
    /// Host directory to share.
    pub source: PathBuf,
    /// If true, guest cannot write to this share.
    pub read_only: bool,
}

/// How one NIC attaches to the outside world.
#[derive(Debug, Clone)]
pub enum NetworkConfig {
    /// NAT networking — guest gets internet through host.
    Nat,
    /// The guest NIC's Ethernet frames are carried over a host-side socket.
    ///
    /// This is the attachment an Environment-owned switch uses: the host end
    /// stays in the runtime, so two Environments never share an L2 segment the
    /// way they share Apple's NAT bridge.
    FileHandle(FileHandleNetwork),
}

/// Apple's documented MTU range for a file-handle network attachment.
const MIN_MTU: u32 = 1500;
const MAX_MTU: u32 = 65535;
/// Socket buffer sizing. Apple requires the receive buffer to be at least twice
/// the send buffer and recommends four times; a buffer too small to hold whole
/// frames drops packets under load instead of applying backpressure, because the
/// transport is datagrams. The floor keeps a jumbo MTU from being sized below a
/// useful number of frames.
const MIN_SEND_BUFFER_BYTES: u32 = 1024 * 1024;
const SEND_BUFFER_FRAMES: u32 = 16;
const RECEIVE_BUFFER_MULTIPLE: u32 = 4;
/// The smallest granted buffer that can still hold whole frames. macOS may clamp
/// a requested size, so the grant is read back and checked rather than assumed.
const MIN_GRANTED_BUFFER_FRAMES: u32 = 4;

/// A connected datagram socket carrying one guest NIC's Ethernet frames.
///
/// `VZFileHandleNetworkDeviceAttachment` writes one frame per datagram and
/// expects a fixed peer, so an unconnected socket silently discards traffic
/// rather than failing. Both properties are checked here, at the point the
/// caller hands over the socket, instead of surfacing as a guest with no
/// network much later.
///
/// The socket is reference-counted because [`VmConfig`] is cloned before the VM
/// is built, and the VM retains its configuration for its whole life, which is
/// what keeps the descriptor open for as long as the guest can send on it.
#[derive(Debug, Clone)]
pub struct FileHandleNetwork {
    socket: std::sync::Arc<std::os::fd::OwnedFd>,
    mtu: u32,
    granted_send_buffer_bytes: u32,
    granted_receive_buffer_bytes: u32,
}

impl FileHandleNetwork {
    /// Adopt a connected datagram socket as one guest NIC's frame transport.
    ///
    /// Takes ownership of the descriptor, verifies it is the kind of socket the
    /// framework requires, and sizes its buffers for `mtu`. Returns the sizes the
    /// kernel actually granted, which can be smaller than requested.
    pub fn new(socket: std::os::fd::OwnedFd, mtu: u32) -> Result<Self, VzError> {
        use std::os::fd::AsRawFd;

        if !(MIN_MTU..=MAX_MTU).contains(&mtu) {
            return Err(VzError::InvalidConfig(format!(
                "network MTU {mtu} is outside the supported range {MIN_MTU}..={MAX_MTU}"
            )));
        }
        let raw = socket.as_raw_fd();
        let socket_type = getsockopt_int(raw, libc::SO_TYPE)?;
        if socket_type != libc::SOCK_DGRAM {
            return Err(VzError::InvalidConfig(format!(
                "network attachment requires a datagram socket; SO_TYPE is {socket_type}"
            )));
        }
        require_connected(raw)?;

        let send = MIN_SEND_BUFFER_BYTES.max(mtu.saturating_mul(SEND_BUFFER_FRAMES));
        let receive = send.saturating_mul(RECEIVE_BUFFER_MULTIPLE);
        setsockopt_int(raw, libc::SO_SNDBUF, send)?;
        setsockopt_int(raw, libc::SO_RCVBUF, receive)?;
        let granted_send = u32::try_from(getsockopt_int(raw, libc::SO_SNDBUF)?).unwrap_or(0);
        let granted_receive = u32::try_from(getsockopt_int(raw, libc::SO_RCVBUF)?).unwrap_or(0);
        let floor = mtu.saturating_mul(MIN_GRANTED_BUFFER_FRAMES);
        if granted_send < floor || granted_receive < floor {
            return Err(VzError::InvalidConfig(format!(
                "kernel granted send {granted_send} and receive {granted_receive} socket buffer \
                 bytes, below the {floor} needed to hold whole {mtu}-byte frames"
            )));
        }
        if granted_receive < granted_send.saturating_mul(2) {
            return Err(VzError::InvalidConfig(format!(
                "granted receive buffer {granted_receive} is below twice the granted send buffer \
                 {granted_send}, which the file-handle attachment requires"
            )));
        }
        Ok(Self {
            socket: std::sync::Arc::new(socket),
            mtu,
            granted_send_buffer_bytes: granted_send,
            granted_receive_buffer_bytes: granted_receive,
        })
    }

    /// The MTU the guest NIC is configured with.
    pub fn mtu(&self) -> u32 {
        self.mtu
    }

    /// The send buffer size the kernel granted, in bytes.
    pub fn granted_send_buffer_bytes(&self) -> u32 {
        self.granted_send_buffer_bytes
    }

    /// The receive buffer size the kernel granted, in bytes.
    pub fn granted_receive_buffer_bytes(&self) -> u32 {
        self.granted_receive_buffer_bytes
    }

    pub(crate) fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.socket.as_raw_fd()
    }
}

fn getsockopt_int(fd: std::os::fd::RawFd, option: libc::c_int) -> Result<libc::c_int, VzError> {
    let mut value: libc::c_int = 0;
    let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `value` and `length` are a correctly sized c_int and its length,
    // and `fd` is borrowed from an OwnedFd that outlives this call.
    let status = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            std::ptr::from_mut(&mut value).cast::<libc::c_void>(),
            &mut length,
        )
    };
    if status != 0 {
        return Err(VzError::InvalidConfig(format!(
            "cannot read socket option {option}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(value)
}

fn setsockopt_int(fd: std::os::fd::RawFd, option: libc::c_int, value: u32) -> Result<(), VzError> {
    let value = libc::c_int::try_from(value).unwrap_or(libc::c_int::MAX);
    // SAFETY: `value` is a live c_int and its length is passed exactly; `fd` is
    // borrowed from an OwnedFd that outlives this call.
    let status = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            std::ptr::from_ref(&value).cast::<libc::c_void>(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if status != 0 {
        return Err(VzError::InvalidConfig(format!(
            "cannot set socket option {option} to {value}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn require_connected(fd: std::os::fd::RawFd) -> Result<(), VzError> {
    let mut address = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: the buffer is a zeroed sockaddr_storage and `length` is its exact
    // size, which is what getpeername requires; `fd` outlives this call.
    let status = unsafe {
        libc::getpeername(
            fd,
            address.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut length,
        )
    };
    if status != 0 {
        return Err(VzError::InvalidConfig(format!(
            "network attachment requires a connected socket; getpeername failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// One virtual NIC: how it attaches to the outside, and the address the guest
/// presents on it.
#[derive(Debug, Clone)]
pub struct Nic {
    attachment: NetworkConfig,
    mac: Option<String>,
}

impl Nic {
    /// A NAT-attached NIC with a generated address.
    pub fn nat() -> Self {
        Self {
            attachment: NetworkConfig::Nat,
            mac: None,
        }
    }

    /// A NIC whose frames are carried over a host-side socket.
    pub fn file_handle(network: FileHandleNetwork) -> Self {
        Self {
            attachment: NetworkConfig::FileHandle(network),
            mac: None,
        }
    }

    /// Pin this NIC's MAC to `"XX:XX:XX:XX:XX:XX"` (six hex bytes,
    /// colon-separated).
    ///
    /// When unset, `build()` generates a fresh random locally-administered MAC.
    /// Because an unpinned address is randomized on every construction, a
    /// restored VM would not match the NIC its saved guest expects, and two VMs
    /// in one process would collide on the host bridge. Pinning is what lets a
    /// consumer derive a deterministic MAC from a stable VM identity (e.g., for
    /// traffic correlation across restarts).
    pub fn with_mac(mut self, mac: impl Into<String>) -> Self {
        self.mac = Some(mac.into());
        self
    }

    /// The attachment this NIC uses.
    pub fn attachment(&self) -> &NetworkConfig {
        &self.attachment
    }
}

/// A NIC with its address resolved. `build()` fills a random
/// locally-administered MAC for any NIC that did not pin one.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedNic {
    pub(crate) attachment: NetworkConfig,
    pub(crate) mac: String,
}

/// One block device attached to the VM.
///
/// Disks are presented to the guest as virtio-block devices in the order
/// they were appended to the builder — the first disk is `vda`, the second
/// `vdb`, and so on. Apple's `setStorageDevices_` accepts an array of
/// configurations, and consumers running structured microVM workloads
/// typically need an ordered set (rootfs, data, metadata, override) rather
/// than a single image with a partition table.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Stable identifier used for logging and (future) hot-replace flows.
    /// Not visible to the guest — the guest sees `vda`/`vdb`/... by order.
    pub id: String,
    /// Host-side path to the disk image.
    pub path: PathBuf,
    /// If true, the guest cannot write to this disk.
    pub read_only: bool,
}

/// Builder for VM configuration.
#[derive(Debug)]
pub struct VmConfigBuilder {
    cpus: u32,
    memory_bytes: u64,
    boot_loader: Option<BootLoader>,
    mac_platform: Option<MacPlatformConfig>,
    disks: Vec<DiskConfig>,
    shared_dirs: Vec<SharedDirConfig>,
    serial_log_file: Option<PathBuf>,
    generic_machine_identifier: Option<Vec<u8>>,
    nics: Vec<Nic>,
    vsock: bool,
    headless: bool,
    nested_virtualization: bool,
    memory_balloon: bool,
    entropy_device: bool,
}

impl VmConfigBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            cpus: 2,
            memory_bytes: 4 * 1024 * 1024 * 1024, // 4 GB
            boot_loader: None,
            mac_platform: None,
            disks: Vec::new(),
            shared_dirs: Vec::new(),
            serial_log_file: None,
            generic_machine_identifier: None,
            nics: vec![Nic::nat()],
            vsock: false,
            headless: true,
            nested_virtualization: true,
            memory_balloon: true,
            entropy_device: true,
        }
    }

    /// Set number of CPU cores.
    pub fn cpus(mut self, cpus: u32) -> Self {
        self.cpus = cpus;
        self
    }

    /// Set memory in gigabytes.
    pub fn memory_gb(mut self, gb: u32) -> Self {
        self.memory_bytes = u64::from(gb) * 1024 * 1024 * 1024;
        self
    }

    /// Set memory in megabytes.
    pub fn memory_mb(mut self, mb: u64) -> Self {
        self.memory_bytes = mb * 1024 * 1024;
        self
    }

    /// Set memory in bytes.
    pub fn memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = bytes;
        self
    }

    /// Set the boot loader.
    pub fn boot_loader(mut self, loader: BootLoader) -> Self {
        self.boot_loader = Some(loader);
        self
    }

    /// Convenience: configure macOS boot.
    pub fn boot_macos(mut self) -> Self {
        self.boot_loader = Some(BootLoader::MacOS);
        self
    }

    /// Convenience: configure Linux boot.
    pub fn boot_linux<K, I, C>(mut self, kernel: K, initrd: Option<I>, cmdline: C) -> Self
    where
        K: Into<PathBuf>,
        I: Into<PathBuf>,
        C: Into<String>,
    {
        self.boot_loader = Some(BootLoader::Linux {
            kernel: kernel.into(),
            initrd: initrd.map(Into::into),
            cmdline: cmdline.into(),
        });
        self
    }

    /// Set the macOS platform configuration.
    ///
    /// Required when using `BootLoader::MacOS`. Provides the hardware model,
    /// machine identifier, and auxiliary storage paths created during installation.
    pub fn mac_platform(mut self, config: MacPlatformConfig) -> Self {
        self.mac_platform = Some(config);
        self
    }

    /// Append a disk to the VM's storage devices.
    ///
    /// Disks appear in the guest as virtio-block devices in declaration order:
    /// the first appended disk is `vda`, the second `vdb`, and so on. For
    /// macOS guests the first disk must be the rootfs.
    pub fn disk(mut self, disk: DiskConfig) -> Self {
        self.disks.push(disk);
        self
    }

    /// Add a shared directory (VirtioFS).
    pub fn shared_dir(mut self, config: SharedDirConfig) -> Self {
        self.shared_dirs.push(config);
        self
    }

    /// Add multiple shared directories (VirtioFS).
    pub fn shared_dirs(mut self, configs: Vec<SharedDirConfig>) -> Self {
        self.shared_dirs.extend(configs);
        self
    }

    /// Write guest serial console output to a host file.
    pub fn serial_log_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.serial_log_file = Some(path.into());
        self
    }

    /// Set a persisted generic machine identifier for Linux VM save/restore.
    pub fn generic_machine_identifier(mut self, machine_identifier: Vec<u8>) -> Self {
        self.generic_machine_identifier = Some(machine_identifier);
        self
    }

    /// Replace the VM's NICs, which the guest sees in declaration order.
    ///
    /// The default is a single NAT NIC; passing an empty list leaves the guest
    /// with no network at all.
    pub fn nics(mut self, nics: impl IntoIterator<Item = Nic>) -> Self {
        self.nics = nics.into_iter().collect();
        self
    }

    /// Enable vsock for host↔guest communication.
    pub fn enable_vsock(mut self) -> Self {
        self.vsock = true;
        self
    }

    /// Run with a display (for debugging). Default is headless.
    pub fn with_display(mut self) -> Self {
        self.headless = false;
        self
    }

    /// Enable nested virtualization for Linux guests.
    ///
    /// When enabled, the guest VM exposes `/dev/kvm`, allowing it to run
    /// hypervisors like Firecracker or Cloud Hypervisor inside the guest.
    /// Only supported on `VZGenericPlatformConfiguration` (Linux guests).
    /// Requires Apple Silicon with Virtualization.framework support.
    pub fn nested_virtualization(mut self, enabled: bool) -> Self {
        self.nested_virtualization = enabled;
        self
    }

    /// Enable or disable the virtio memory balloon device. Default: enabled.
    ///
    /// When enabled, the host can call [`Vm::set_target_memory_size`] at runtime
    /// to ask the guest to release pages back to the host (or to give them back).
    /// Apple's framework allows at most one balloon device per VM, so this is
    /// a simple on/off knob — there is nothing else to configure.
    pub fn memory_balloon(mut self, enabled: bool) -> Self {
        self.memory_balloon = enabled;
        self
    }

    /// Enable or disable the virtio entropy device. Default: enabled.
    ///
    /// Linux guests use this device to seed the kernel random number
    /// generator. Without it, early user-space programs that call
    /// `getrandom(2)` can block indefinitely in tiny VM images with no
    /// other entropy source.
    pub fn entropy_device(mut self, enabled: bool) -> Self {
        self.entropy_device = enabled;
        self
    }

    /// Validate and build the configuration.
    pub fn build(self) -> Result<VmConfig, VzError> {
        let boot_loader = self
            .boot_loader
            .ok_or_else(|| VzError::InvalidConfig("boot loader is required".into()))?;

        // macOS boot requires platform configuration
        if matches!(boot_loader, BootLoader::MacOS) && self.mac_platform.is_none() {
            return Err(VzError::InvalidConfig(
                "macOS boot loader requires mac_platform configuration".into(),
            ));
        }

        // macOS must boot from disk; Linux can boot from initramfs alone.
        if matches!(boot_loader, BootLoader::MacOS) && self.disks.is_empty() {
            return Err(VzError::InvalidConfig(
                "macOS boot requires at least one disk".into(),
            ));
        }

        // Each NIC resolves its own address so two NICs on one VM never share one.
        let nics = self
            .nics
            .into_iter()
            .map(|nic| ResolvedNic {
                attachment: nic.attachment,
                mac: nic
                    .mac
                    .unwrap_or_else(crate::bridge::random_locally_administered_mac_string),
            })
            .collect();

        Ok(VmConfig {
            cpus: self.cpus,
            memory_bytes: self.memory_bytes,
            boot_loader,
            mac_platform: self.mac_platform,
            disks: self.disks,
            shared_dirs: self.shared_dirs,
            serial_log_file: self.serial_log_file,
            generic_machine_identifier: self.generic_machine_identifier,
            nics,
            vsock: self.vsock,
            headless: self.headless,
            nested_virtualization: self.nested_virtualization,
            memory_balloon: self.memory_balloon,
            entropy_device: self.entropy_device,
        })
    }
}

impl Default for VmConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Validated VM configuration, ready to create a VM.
#[derive(Debug, Clone)]
pub struct VmConfig {
    pub(crate) cpus: u32,
    pub(crate) memory_bytes: u64,
    pub(crate) boot_loader: BootLoader,
    pub(crate) mac_platform: Option<MacPlatformConfig>,
    /// Block devices in declaration order — guest sees them as `vda`, `vdb`, ...
    pub(crate) disks: Vec<DiskConfig>,
    pub(crate) shared_dirs: Vec<SharedDirConfig>,
    pub(crate) serial_log_file: Option<PathBuf>,
    pub(crate) generic_machine_identifier: Option<Vec<u8>>,
    /// NICs in declaration order, each with a MAC always populated by `build()`.
    /// Persisting the addresses here keeps save/restore correct (a restored VM's
    /// NIC must match the MAC the saved guest expects) and gives every NIC a
    /// unique address, whether it shares a VM or a process with the others.
    pub(crate) nics: Vec<ResolvedNic>,
    pub(crate) vsock: bool,
    /// Controls whether to attach a virtual display. Used by CLI layer.
    #[allow(dead_code)]
    pub(crate) headless: bool,
    /// Enable nested virtualization (exposes /dev/kvm in Linux guests).
    pub(crate) nested_virtualization: bool,
    /// Attach a virtio memory balloon device. Required for runtime memory
    /// reclaim via [`Vm::set_target_memory_size`].
    pub(crate) memory_balloon: bool,
    /// Attach a virtio entropy device. Required for reliable early
    /// getrandom(2) in minimal Linux guests.
    pub(crate) entropy_device: bool,
}

impl VmConfig {
    /// Read the MAC addresses of this VM's NICs, in declaration order.
    ///
    /// Every entry has a value: either what the caller pinned via
    /// [`Nic::with_mac`], or a fresh random locally-administered address
    /// generated at `build()` time.
    pub fn mac_addresses(&self) -> Vec<&str> {
        self.nics.iter().map(|nic| nic.mac.as_str()).collect()
    }

    /// Read the ordered list of disks attached to this VM.
    ///
    /// The first entry is `vda` in the guest, the second `vdb`, and so on.
    pub fn disks(&self) -> &[DiskConfig] {
        &self.disks
    }

    /// Whether this VM was built with a virtio memory balloon device.
    ///
    /// When `true`, the host can call [`Vm::set_target_memory_size`] to ask
    /// the guest to release pages back to the host.
    pub fn memory_balloon_enabled(&self) -> bool {
        self.memory_balloon
    }

    /// Whether this VM was built with a virtio entropy device.
    pub fn entropy_device_enabled(&self) -> bool {
        self.entropy_device
    }
}
