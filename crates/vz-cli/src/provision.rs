//! Automated first-boot provisioning for golden images.
//!
//! After macOS installation (`VZMacOSInstaller`), this module handles:
//! - Skipping Setup Assistant (`.AppleSetupDone`)
//! - Pre-creating the `dev` user account (UID 501)
//! - Enabling auto-login
//! - Installing the guest agent binary + launchd plist
//! - Running dev tool provisioning via the guest agent

use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Configuration for the dev user account created in the VM.
#[derive(Debug, Clone)]
pub struct UserConfig {
    /// Username (default: "dev").
    pub username: String,
    /// Password (default: "dev").
    pub password: String,
    /// User ID (default: 501, macOS first user).
    pub uid: u32,
    /// Primary group ID (default: 20, "staff" on macOS).
    pub gid: u32,
    /// Login shell.
    pub shell: String,
    /// Home directory (inside the VM).
    pub home: String,
}

impl Default for UserConfig {
    fn default() -> Self {
        Self {
            username: "dev".to_string(),
            password: generate_random_password(),
            uid: 501,
            gid: 20,
            shell: "/bin/zsh".to_string(),
            home: "/Users/dev".to_string(),
        }
    }
}

/// Generate a random 8-character hex password from /dev/urandom.
fn generate_random_password() -> String {
    let mut buf = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut buf);
    }
    format!("{:02x}{:02x}{:02x}{:02x}", buf[0], buf[1], buf[2], buf[3])
}

/// What dev tools to install during provisioning (Phase 2, via guest agent).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProvisionConfig {
    /// Install Xcode Command Line Tools.
    pub xcode_cli: bool,
    /// Install Homebrew.
    pub homebrew: bool,
    /// Install Rust toolchain via rustup.
    pub rust: bool,
    /// Additional Homebrew packages to install.
    pub brew_packages: Vec<String>,
}

impl Default for ProvisionConfig {
    fn default() -> Self {
        Self {
            xcode_cli: true,
            homebrew: true,
            rust: true,
            brew_packages: vec!["git".to_string(), "cmake".to_string()],
        }
    }
}

/// How the guest agent should be installed for startup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AgentInstallMode {
    /// Install as a system LaunchDaemon (root-owned, starts before login).
    #[default]
    SystemLaunchDaemon,
    /// Install as a per-user LaunchAgent (rootless-friendly, starts at login).
    UserLaunchAgent,
}

// ---------------------------------------------------------------------------
// Auto-configuration (applied to disk image before first boot)
// ---------------------------------------------------------------------------

/// Apply auto-configuration to a mounted guest disk image.
///
/// This prepares the disk image for unattended first boot:
/// - Skips Setup Assistant
/// - Pre-creates the dev user
/// - Enables auto-login
/// - Installs the guest agent
///
/// `mount_point` is where the guest disk image is mounted on the host
/// (e.g., `/Volumes/GuestDisk`).
pub fn apply_auto_config(
    mount_point: &Path,
    user_config: &UserConfig,
    guest_agent_binary: Option<&Path>,
    install_mode: AgentInstallMode,
) -> anyhow::Result<()> {
    info!(
        mount_point = %mount_point.display(),
        user = %user_config.username,
        "applying auto-configuration to disk image"
    );

    let user_home_rel = user_config
        .home
        .strip_prefix('/')
        .unwrap_or(&user_config.home);
    let user_home_exists = mount_point.join(user_home_rel).exists();

    match install_mode {
        AgentInstallMode::SystemLaunchDaemon => {
            skip_setup_assistant(mount_point)?;
            create_user_account(mount_point, user_config)?;
            enable_auto_login(mount_point, &user_config.username, &user_config.password)?;
        }
        AgentInstallMode::UserLaunchAgent => {
            if let Err(error) = skip_setup_assistant(mount_point) {
                warn!(
                    error = %error,
                    "failed to write .AppleSetupDone in user mode; continuing"
                );
            }

            if let Err(error) = create_user_account(mount_point, user_config) {
                if user_home_exists {
                    warn!(
                        error = %error,
                        user = %user_config.username,
                        "unable to modify dslocal in user mode; existing user home found, continuing"
                    );
                } else {
                    return Err(error);
                }
            }

            if let Err(error) =
                enable_auto_login(mount_point, &user_config.username, &user_config.password)
            {
                warn!(
                    error = %error,
                    "failed to set auto-login in user mode; continuing"
                );
            }
        }
    }

    if let Some(agent_path) = guest_agent_binary {
        install_guest_agent(mount_point, agent_path, user_config, install_mode)?;
    }

    info!("auto-configuration complete");
    Ok(())
}

/// Create the `.AppleSetupDone` marker file to skip Setup Assistant.
fn skip_setup_assistant(mount_point: &Path) -> anyhow::Result<()> {
    let marker_dir = mount_point.join("private/var/db");
    std::fs::create_dir_all(&marker_dir)?;

    let marker = marker_dir.join(".AppleSetupDone");
    std::fs::write(&marker, "")?;

    debug!(path = %marker.display(), "created .AppleSetupDone marker");
    Ok(())
}

/// Pre-create a user account on the mounted disk image.
///
/// Writes a dslocal plist directly to the user database on disk.
/// Each user is a plist file at `<mount>/private/var/db/dslocal/nodes/Default/users/<name>.plist`.
fn create_user_account(mount_point: &Path, config: &UserConfig) -> anyhow::Result<()> {
    let users_dir = mount_point.join("private/var/db/dslocal/nodes/Default/users");

    // The Default directory may lack the execute bit; fix it so we can write into it
    let default_dir = mount_point.join("private/var/db/dslocal/nodes/Default");
    if default_dir.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(&default_dir)?;
            let mode = meta.permissions().mode();
            if mode & 0o100 == 0 {
                std::fs::set_permissions(
                    &default_dir,
                    std::fs::Permissions::from_mode(mode | 0o100),
                )?;
            }
        }
    }

    if !users_dir.exists() {
        anyhow::bail!(
            "dslocal users directory not found at {}. Is the disk image mounted correctly?",
            users_dir.display()
        );
    }

    // Generate a deterministic UUID for the user
    let generated_uid = format!(
        "{:08X}-0000-0000-0000-{:012X}",
        config.uid, config.uid as u64
    );

    // Generate ShadowHashData (PBKDF2-SHA512 password hash)
    let shadow_hash_b64 = generate_shadow_hash_data(&config.password)?;

    // Write the user plist (dslocal format: all values are arrays)
    let plist_content = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>ShadowHashData</key>
    <array>
        <data>{shadow_hash_data}</data>
    </array>
    <key>authentication_authority</key>
    <array>
        <string>;ShadowHash;HASHLIST:&lt;SALTED-SHA512-PBKDF2&gt;</string>
    </array>
    <key>generateduid</key>
    <array>
        <string>{generated_uid}</string>
    </array>
    <key>gid</key>
    <array>
        <string>{gid}</string>
    </array>
    <key>home</key>
    <array>
        <string>{home}</string>
    </array>
    <key>name</key>
    <array>
        <string>{username}</string>
    </array>
    <key>passwd</key>
    <array>
        <string>********</string>
    </array>
    <key>realname</key>
    <array>
        <string>{username}</string>
    </array>
    <key>shell</key>
    <array>
        <string>{shell}</string>
    </array>
    <key>uid</key>
    <array>
        <string>{uid}</string>
    </array>
</dict>
</plist>
"#,
        shadow_hash_data = shadow_hash_b64,
        generated_uid = generated_uid,
        gid = config.gid,
        home = config.home,
        username = config.username,
        shell = config.shell,
        uid = config.uid,
    );

    let plist_path = users_dir.join(format!("{}.plist", config.username));
    std::fs::write(&plist_path, &plist_content)?;

    // Match permissions of other user plists (600)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&plist_path, std::fs::Permissions::from_mode(0o600))?;
    }

    debug!(plist = %plist_path.display(), "wrote user plist");

    // Create home directory
    let home_dir = mount_point.join(config.home.strip_prefix('/').unwrap_or(&config.home));
    std::fs::create_dir_all(&home_dir)?;

    info!(
        user = %config.username,
        uid = config.uid,
        "created user account on disk image"
    );
    Ok(())
}

/// Enable auto-login for a user via loginwindow preferences and kcpassword.
///
/// macOS auto-login requires two things:
/// 1. `com.apple.loginwindow.plist` with `autoLoginUser` key
/// 2. `/etc/kcpassword` with the XOR-obfuscated password
///
/// Without both, the login screen appears and no user session starts.
fn enable_auto_login(mount_point: &Path, username: &str, password: &str) -> anyhow::Result<()> {
    // Set autoLoginUser in loginwindow plist
    let plist_path = mount_point.join("Library/Preferences/com.apple.loginwindow.plist");
    if let Some(parent) = plist_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let output = std::process::Command::new("defaults")
        .args([
            "write",
            &plist_path.to_string_lossy(),
            "autoLoginUser",
            "-string",
            username,
        ])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "defaults write failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    // Generate kcpassword for auto-login
    let kcpassword_path = mount_point.join("private/etc/kcpassword");
    if let Some(parent) = kcpassword_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let kcpassword_bytes = encode_kcpassword(password);
    std::fs::write(&kcpassword_path, &kcpassword_bytes)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&kcpassword_path, std::fs::Permissions::from_mode(0o600))?;
    }

    debug!(user = username, "enabled auto-login with kcpassword");
    Ok(())
}

/// Generate ShadowHashData for a user password.
///
/// Uses PBKDF2-HMAC-SHA512 (macOS standard) to hash the password and produces
/// a base64-encoded binary plist suitable for the `ShadowHashData` field in a
/// dslocal user plist.
///
/// Shells out to python3 (always available on macOS) to compute the PBKDF2 hash
/// and create the binary plist format.
fn generate_shadow_hash_data(password: &str) -> anyhow::Result<String> {
    let script = r#"
import hashlib, os, plistlib, base64, sys
password = sys.argv[1]
salt = os.urandom(32)
iterations = 40000
entropy = hashlib.pbkdf2_hmac('sha512', password.encode('utf-8'), salt, iterations, dklen=128)
shadow_hash = {
    'SALTED-SHA512-PBKDF2': {
        'entropy': entropy,
        'salt': salt,
        'iterations': iterations,
    }
}
binary_plist = plistlib.dumps(shadow_hash, fmt=plistlib.FMT_BINARY)
print(base64.b64encode(binary_plist).decode())
"#;

    let output = std::process::Command::new("python3")
        .args(["-c", script, password])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("failed to generate ShadowHashData: {}", stderr.trim());
    }

    let b64 = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if b64.is_empty() {
        anyhow::bail!("ShadowHashData generation produced empty output");
    }

    debug!(len = b64.len(), "generated ShadowHashData");
    Ok(b64)
}

/// Encode a password using macOS kcpassword XOR cipher.
///
/// The kcpassword file uses a simple repeating XOR key to obfuscate the
/// password. This is a well-known, reversible encoding (not encryption).
fn encode_kcpassword(password: &str) -> Vec<u8> {
    const KEY: [u8; 11] = [
        0x7D, 0x89, 0x52, 0x23, 0xD2, 0xBC, 0xDD, 0xEA, 0xA3, 0xB9, 0x1F,
    ];

    let pass_bytes = password.as_bytes();

    // Pad password to next multiple of key length (minimum 1 block)
    let padded_len = ((pass_bytes.len() / KEY.len()) + 1) * KEY.len();
    let mut padded = vec![0u8; padded_len];
    padded[..pass_bytes.len()].copy_from_slice(pass_bytes);

    // XOR with repeating key
    for (i, byte) in padded.iter_mut().enumerate() {
        *byte ^= KEY[i % KEY.len()];
    }

    padded
}

/// Install the guest agent binary and launchd plist into the disk image.
///
/// Supports both system LaunchDaemon and per-user LaunchAgent modes.
fn install_guest_agent(
    mount_point: &Path,
    agent_binary: &Path,
    user_config: &UserConfig,
    install_mode: AgentInstallMode,
) -> anyhow::Result<()> {
    match install_mode {
        AgentInstallMode::SystemLaunchDaemon => {
            install_guest_agent_system(mount_point, agent_binary)
        }
        AgentInstallMode::UserLaunchAgent => {
            install_guest_agent_user(mount_point, agent_binary, user_config)
        }
    }
}

/// Install the guest agent as a system LaunchDaemon.
fn install_guest_agent_system(mount_point: &Path, agent_binary: &Path) -> anyhow::Result<()> {
    if !agent_binary.exists() {
        anyhow::bail!("guest agent binary not found: {}", agent_binary.display());
    }

    // Install binary
    let bin_dir = mount_point.join("usr/local/bin");
    std::fs::create_dir_all(&bin_dir)?;
    let dest_binary = bin_dir.join("vz-guest-agent");
    std::fs::copy(agent_binary, &dest_binary)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest_binary, std::fs::Permissions::from_mode(0o755))?;
    }

    // Install launchd plist
    let launch_daemons = mount_point.join("Library/LaunchDaemons");
    std::fs::create_dir_all(&launch_daemons)?;
    let plist_path = launch_daemons.join("com.vz.guest-agent.plist");
    std::fs::write(&plist_path, GUEST_AGENT_LAUNCHD_PLIST)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&plist_path, std::fs::Permissions::from_mode(0o644))?;
    }

    // launchd requires LaunchDaemon plists and binaries to be owned by root:wheel.
    // When running as root, explicitly chown since APFS disk images may ignore ownership.
    #[cfg(unix)]
    if is_root() {
        for path in [&dest_binary, &plist_path, &launch_daemons, &bin_dir] {
            std::os::unix::fs::chown(path, Some(0), Some(0)).map_err(|e| {
                anyhow::anyhow!("chown root:wheel failed for {}: {e}", path.display())
            })?;
        }
        debug!("set root:wheel ownership on LaunchDaemon files");
    }

    info!(
        binary = %dest_binary.display(),
        plist = %plist_path.display(),
        "installed guest agent (LaunchDaemon)"
    );
    Ok(())
}

/// Install the guest agent as a per-user LaunchAgent.
fn install_guest_agent_user(
    mount_point: &Path,
    agent_binary: &Path,
    user_config: &UserConfig,
) -> anyhow::Result<()> {
    if !agent_binary.exists() {
        anyhow::bail!("guest agent binary not found: {}", agent_binary.display());
    }

    let user_home_rel = user_config
        .home
        .strip_prefix('/')
        .unwrap_or(&user_config.home);
    let user_home = mount_point.join(user_home_rel);
    let guest_binary_path = PathBuf::from(&user_config.home)
        .join("Library")
        .join("Application Support")
        .join("vz")
        .join("vz-guest-agent");

    let agent_dir = user_home.join("Library/Application Support/vz");
    std::fs::create_dir_all(&agent_dir)?;
    let dest_binary = agent_dir.join("vz-guest-agent");
    std::fs::copy(agent_binary, &dest_binary)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest_binary, std::fs::Permissions::from_mode(0o755))?;
    }

    let launch_agents = user_home.join("Library/LaunchAgents");
    std::fs::create_dir_all(&launch_agents)?;
    let plist_path = launch_agents.join("com.vz.user-guest-agent.plist");
    let plist = guest_agent_launchagent_plist(&guest_binary_path);
    std::fs::write(&plist_path, plist)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&plist_path, std::fs::Permissions::from_mode(0o644))?;
    }

    info!(
        user = %user_config.username,
        binary = %dest_binary.display(),
        plist = %plist_path.display(),
        "installed guest agent (LaunchAgent)"
    );
    Ok(())
}

fn guest_agent_launchagent_plist(program_path: &Path) -> String {
    let program = program_path.to_string_lossy();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.vz.user-guest-agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>{program}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/vz-guest-agent.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/vz-guest-agent.log</string>
</dict>
</plist>
"#
    )
}

/// launchd plist for the guest agent daemon.
const GUEST_AGENT_LAUNCHD_PLIST: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.vz.guest-agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/vz-guest-agent</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/vz-guest-agent.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/vz-guest-agent.log</string>
</dict>
</plist>
"#;

// ---------------------------------------------------------------------------
// Dev tool provisioning (runs inside VM via guest agent)
// ---------------------------------------------------------------------------

/// Generate the provisioning shell script based on the config.
///
/// This script is executed inside the VM via the guest agent after
/// first boot completes. It installs dev tools non-interactively.
#[allow(dead_code)]
pub fn generate_provision_script(config: &ProvisionConfig) -> String {
    let mut script = String::from("#!/bin/bash\nset -euo pipefail\n\n");

    if config.xcode_cli {
        script.push_str(
            "# Install Xcode Command Line Tools\n\
             echo 'Installing Xcode Command Line Tools...'\n\
             sudo xcodebuild -license accept 2>/dev/null || true\n\
             xcode-select --install 2>/dev/null || true\n\
             # Wait for installation to complete (up to 10 minutes)\n\
             for i in $(seq 1 120); do\n\
               if xcode-select -p &>/dev/null; then break; fi\n\
               sleep 5\n\
             done\n\
             echo 'Xcode CLI tools installed.'\n\n",
        );
    }

    if config.homebrew {
        script.push_str(
            "# Install Homebrew\n\
             echo 'Installing Homebrew...'\n\
             NONINTERACTIVE=1 /bin/bash -c \\\n\
               \"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\"\n\
             eval \"$(/opt/homebrew/bin/brew shellenv)\"\n\
             echo 'eval \"$(/opt/homebrew/bin/brew shellenv)\"' >> ~/.zprofile\n\
             echo 'Homebrew installed.'\n\n",
        );

        if !config.brew_packages.is_empty() {
            let packages = config.brew_packages.join(" ");
            script.push_str(&format!(
                "# Install common dev tools\n\
                 echo 'Installing packages: {packages}...'\n\
                 brew install {packages}\n\
                 echo 'Packages installed.'\n\n",
            ));
        }
    }

    if config.rust {
        script.push_str(
            "# Install Rust toolchain\n\
             echo 'Installing Rust...'\n\
             curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y\n\
             source \"$HOME/.cargo/env\"\n\
             echo 'Rust installed.'\n\n",
        );
    }

    script.push_str("echo 'Provisioning complete.'\n");
    script
}

/// Read the saved password for a disk image (from the `.password` sidecar file).
pub fn read_saved_password(image_path: &Path) -> Option<String> {
    let password_path = image_path.with_extension("password");
    std::fs::read_to_string(&password_path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Find the built guest agent binary.
///
/// Checks common locations:
/// 1. Relative to the current executable (sibling binary in same target dir)
/// 2. `target/release/vz-guest-agent` and `target/debug/vz-guest-agent` (cwd)
/// 3. System path via `which`
pub fn find_guest_agent_binary() -> Option<PathBuf> {
    // Check next to the running binary (e.g., target/debug/vz-guest-agent)
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join("vz-guest-agent");
            if sibling.exists() {
                return Some(sibling);
            }
        }
    }

    // Check target directories relative to cwd
    let target_dirs = [
        PathBuf::from("target/release/vz-guest-agent"),
        PathBuf::from("target/debug/vz-guest-agent"),
    ];

    for path in &target_dirs {
        if path.exists() {
            return Some(path.clone());
        }
    }

    // Check system path
    let output = std::process::Command::new("which")
        .arg("vz-guest-agent")
        .output()
        .ok()?;

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Some(PathBuf::from(path));
        }
    }

    None
}

/// Check if the current process is running as root.
#[cfg(unix)]
#[allow(unsafe_code)]
fn is_root() -> bool {
    // SAFETY: geteuid() is a trivial syscall with no side effects.
    unsafe { libc::geteuid() == 0 }
}

// ---------------------------------------------------------------------------
// Disk image mount/unmount
// ---------------------------------------------------------------------------

/// State of an attached raw disk image.
pub struct AttachedDisk {
    /// The device node (e.g., "disk4").
    pub device: String,
    /// The mount point where the data volume is mounted.
    pub mount_point: PathBuf,
    /// The data volume device (e.g., "disk7s5").
    _data_volume: String,
}

impl AttachedDisk {
    /// Unmount and detach the disk image.
    pub fn detach(self) -> anyhow::Result<()> {
        detach_disk_image(&self.device, &self.mount_point)
    }
}

/// Attach a raw disk image and mount its APFS data volume.
///
/// Uses `hdiutil attach` with `-imagekey diskimage-class=CRawDiskImage` to
/// attach the raw VM disk, then finds and mounts the APFS Data volume.
pub fn attach_and_mount(image_path: &Path) -> anyhow::Result<AttachedDisk> {
    info!(image = %image_path.display(), "attaching disk image");

    // Attach without mounting (we'll mount the specific volume we need)
    let output = std::process::Command::new("hdiutil")
        .args([
            "attach",
            "-imagekey",
            "diskimage-class=CRawDiskImage",
            "-nomount",
        ])
        .arg(image_path)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("hdiutil attach failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse the first line to get the base device (e.g., "/dev/disk4")
    let base_device = stdout
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| anyhow::anyhow!("failed to parse hdiutil output"))?
        .trim_start_matches("/dev/")
        .to_string();

    debug!(device = %base_device, "disk image attached");

    // Find the APFS data volume using diskutil
    let data_volume = find_apfs_data_volume(&base_device)?;
    debug!(volume = %data_volume, "found APFS data volume");

    // Mount the data volume
    let mount_point = PathBuf::from("/tmp/vz-provision");
    std::fs::create_dir_all(&mount_point)?;

    let output = std::process::Command::new("diskutil")
        .args(["mount", "-mountPoint"])
        .arg(&mount_point)
        .arg(&data_volume)
        .output()?;

    if !output.status.success() {
        // Detach on mount failure
        let _ = std::process::Command::new("hdiutil")
            .args(["detach", &base_device])
            .output();
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("diskutil mount failed: {}", stderr.trim());
    }

    // Enable ownership on the volume so file UIDs are respected.
    // By default, APFS volumes from disk images have "ignore ownership" enabled,
    // which maps all files to the mounting user's UID regardless of what's on disk.
    let enable_output = std::process::Command::new("diskutil")
        .args(["enableOwnership", &data_volume])
        .output()?;

    if !enable_output.status.success() {
        warn!(
            "diskutil enableOwnership warning: {}",
            String::from_utf8_lossy(&enable_output.stderr).trim()
        );
    }

    info!(
        mount_point = %mount_point.display(),
        volume = %data_volume,
        "data volume mounted (ownership enabled)"
    );

    Ok(AttachedDisk {
        device: base_device,
        mount_point,
        _data_volume: data_volume,
    })
}

/// Find the APFS Data volume on an attached disk.
///
/// `diskutil list <device>` shows lines like:
///   `2: Apple_APFS Container disk7  28.5 GB  disk4s2`
///
/// The container name (disk7) is what we pass to `diskutil apfs list`
/// (not the physical store identifier disk4s2).
fn find_apfs_data_volume(base_device: &str) -> anyhow::Result<String> {
    let output = std::process::Command::new("diskutil")
        .args(["list", base_device])
        .output()?;

    if !output.status.success() {
        anyhow::bail!("diskutil list {} failed", base_device);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Find Apple_APFS containers (skip ISC and Recovery).
    // Lines look like: "2:  Apple_APFS Container disk7  28.5 GB  disk4s2"
    // We need "disk7" (the container), not "disk4s2" (the physical store).
    let mut containers: Vec<String> = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.contains("Apple_APFS")
            && !trimmed.contains("Apple_APFS_ISC")
            && !trimmed.contains("Apple_APFS_Recovery")
        {
            // Extract the container disk identifier after "Container"
            if let Some(idx) = trimmed.find("Container ") {
                let after = &trimmed[idx + "Container ".len()..];
                if let Some(container) = after.split_whitespace().next() {
                    containers.push(container.to_string());
                }
            }
        }
    }

    // For each container, list its volumes and find the Data role
    for container in &containers {
        let output = std::process::Command::new("diskutil")
            .args(["apfs", "list", container])
            .output()?;

        if !output.status.success() {
            continue;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse lines like: "|   APFS Volume Disk (Role):   disk7s5 (Data)"
        for line in stdout.lines() {
            let trimmed = line.trim().trim_start_matches('|').trim();
            if trimmed.starts_with("APFS Volume Disk (Role):") {
                // After the colon: "disk7s5 (Data)"
                if let Some(after_colon) = trimmed.split(':').nth(1) {
                    let tokens: Vec<&str> = after_colon.split_whitespace().collect();
                    // tokens = ["disk7s5", "(Data)"]
                    if tokens.len() >= 2 && tokens[1] == "(Data)" {
                        return Ok(tokens[0].to_string());
                    }
                }
            }
        }
    }

    anyhow::bail!(
        "no APFS Data volume found on {}. Containers checked: {:?}",
        base_device,
        containers
    )
}

/// Unmount and detach a disk image.
fn detach_disk_image(device: &str, mount_point: &Path) -> anyhow::Result<()> {
    // Unmount first
    let output = std::process::Command::new("diskutil")
        .args(["unmount", &mount_point.to_string_lossy()])
        .output()?;

    if !output.status.success() {
        warn!(
            "diskutil unmount warning: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    // Detach the disk
    let output = std::process::Command::new("hdiutil")
        .args(["detach", device])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("hdiutil detach failed: {}", stderr.trim());
    }

    info!(device = %device, "disk image detached");
    Ok(())
}

// ---------------------------------------------------------------------------
// Top-level provisioning entry point
// ---------------------------------------------------------------------------

/// Provision a raw VM disk image offline.
///
/// Attaches the image, mounts the APFS data volume, applies auto-configuration
/// (skip Setup Assistant, create user, install guest agent), then unmounts and
/// detaches.
/// Result of provisioning indicating if post-provisioning steps are needed.
pub struct ProvisionResult {
    /// If true, LaunchDaemon files need root ownership set.
    /// The caller should run the fix command or inform the user.
    pub needs_ownership_fix: bool,
}

pub fn provision_image(
    image_path: &Path,
    user_config: &UserConfig,
    guest_agent_binary: Option<&Path>,
    install_mode: AgentInstallMode,
) -> anyhow::Result<ProvisionResult> {
    // LaunchDaemon plists must be owned by root:wheel. When running as root,
    // files are created with UID 0 automatically. When not root, we still
    // write the files but warn that ownership needs fixing.
    #[cfg(unix)]
    let needs_ownership_fix = guest_agent_binary.is_some()
        && install_mode == AgentInstallMode::SystemLaunchDaemon
        && !is_root();
    #[cfg(not(unix))]
    let needs_ownership_fix = false;

    // Delete any existing save state — the disk will be modified, invalidating it
    let state_path = image_path.with_extension("state");
    if state_path.exists() {
        info!(path = %state_path.display(), "removing stale save state (disk will be modified)");
        std::fs::remove_file(&state_path)?;
    }

    let disk = attach_and_mount(image_path)?;

    let result = apply_auto_config(
        &disk.mount_point,
        user_config,
        guest_agent_binary,
        install_mode,
    );

    // If running as non-root, try to fix ownership via sudo (best effort)
    if needs_ownership_fix && result.is_ok() {
        let binary_path = disk.mount_point.join("usr/local/bin/vz-guest-agent");
        let plist_path = disk
            .mount_point
            .join("Library/LaunchDaemons/com.vz.guest-agent.plist");
        let daemon_dir = disk.mount_point.join("Library/LaunchDaemons");
        let bin_dir = disk.mount_point.join("usr/local/bin");

        // Try sudo chown — this may fail without a TTY, which is fine
        for path in [&binary_path, &plist_path, &daemon_dir, &bin_dir] {
            let _ = std::process::Command::new("sudo")
                .args(["-n", "chown", "0:0"]) // -n = non-interactive (no password prompt)
                .arg(path)
                .output();
        }

        // Check if it actually worked
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let still_needs_fix = plist_path.metadata().map(|m| m.uid() != 0).unwrap_or(true);
            if still_needs_fix {
                warn!("LaunchDaemon files not owned by root — guest agent won't start until fixed");
            }
        }
    }

    // Always try to detach, even if provisioning failed
    let detach_result = disk.detach();

    // Propagate provisioning error first
    result?;
    detach_result?;

    // Save password to sidecar file so `vz run` can display it.
    // Mode 0644 — this is a local dev VM password, not a production secret.
    let password_path = image_path.with_extension("password");
    match std::fs::write(&password_path, &user_config.password) {
        Ok(()) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Err(error) =
                    std::fs::set_permissions(&password_path, std::fs::Permissions::from_mode(0o644))
                {
                    warn!(
                        path = %password_path.display(),
                        error = %error,
                        "failed to set permissions on credentials sidecar"
                    );
                }
            }
            info!(path = %password_path.display(), "saved credentials");
        }
        Err(error) => {
            warn!(
                path = %password_path.display(),
                error = %error,
                "failed to persist credentials sidecar; continuing"
            );
        }
    }

    info!("image provisioned successfully");
    Ok(ProvisionResult {
        needs_ownership_fix,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_config() {
        let config = UserConfig::default();
        assert_eq!(config.username, "dev");
        assert_eq!(config.password.len(), 8, "password should be 8 hex chars");
        assert!(config.password.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(config.uid, 501);
        assert_eq!(config.gid, 20);
        assert_eq!(config.shell, "/bin/zsh");
        assert_eq!(config.home, "/Users/dev");
    }

    #[test]
    fn default_provision_config() {
        let config = ProvisionConfig::default();
        assert!(config.xcode_cli);
        assert!(config.homebrew);
        assert!(config.rust);
        assert!(config.brew_packages.contains(&"git".to_string()));
        assert!(config.brew_packages.contains(&"cmake".to_string()));
    }

    #[test]
    fn generate_provision_script_full() {
        let config = ProvisionConfig::default();
        let script = generate_provision_script(&config);

        assert!(script.starts_with("#!/bin/bash"));
        assert!(script.contains("set -euo pipefail"));
        assert!(script.contains("xcodebuild -license accept"));
        assert!(script.contains("Homebrew/install/HEAD/install.sh"));
        assert!(script.contains("rustup.rs"));
        assert!(script.contains("brew install git cmake"));
        assert!(script.contains("Provisioning complete."));
    }

    #[test]
    fn generate_provision_script_minimal() {
        let config = ProvisionConfig {
            xcode_cli: false,
            homebrew: false,
            rust: false,
            brew_packages: vec![],
        };
        let script = generate_provision_script(&config);

        assert!(script.starts_with("#!/bin/bash"));
        assert!(!script.contains("xcodebuild"));
        assert!(!script.contains("Homebrew"));
        assert!(!script.contains("rustup"));
        assert!(script.contains("Provisioning complete."));
    }

    #[test]
    fn generate_provision_script_homebrew_only() {
        let config = ProvisionConfig {
            xcode_cli: false,
            homebrew: true,
            rust: false,
            brew_packages: vec!["node".to_string(), "python".to_string()],
        };
        let script = generate_provision_script(&config);

        assert!(script.contains("Homebrew"));
        assert!(script.contains("brew install node python"));
        assert!(!script.contains("xcodebuild"));
        assert!(!script.contains("rustup"));
    }

    #[test]
    fn guest_agent_launchd_plist_valid_xml() {
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("<?xml version="));
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("<plist version=\"1.0\">"));
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("com.vz.guest-agent"));
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("/usr/local/bin/vz-guest-agent"));
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("<key>RunAtLoad</key>"));
        assert!(GUEST_AGENT_LAUNCHD_PLIST.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn encode_kcpassword_empty() {
        let encoded = encode_kcpassword("");
        assert_eq!(encoded.len(), 11); // one block
        // Empty password XOR'd with key = the key itself
        assert_eq!(
            encoded,
            vec![
                0x7D, 0x89, 0x52, 0x23, 0xD2, 0xBC, 0xDD, 0xEA, 0xA3, 0xB9, 0x1F
            ]
        );
    }

    #[test]
    fn encode_kcpassword_dev() {
        let encoded = encode_kcpassword("dev");
        assert_eq!(encoded.len(), 11); // one block (3 chars + 8 padding)
        // "dev" = [0x64, 0x65, 0x76], XOR'd with key prefix [0x7D, 0x89, 0x52]
        assert_eq!(encoded[0], 0x64 ^ 0x7D); // 'd' ^ 0x7D = 0x19
        assert_eq!(encoded[1], 0x65 ^ 0x89); // 'e' ^ 0x89 = 0xEC
        assert_eq!(encoded[2], 0x76 ^ 0x52); // 'v' ^ 0x52 = 0x24
    }

    #[test]
    fn generate_shadow_hash_data_produces_valid_base64() {
        use base64::Engine;

        let b64 = generate_shadow_hash_data("dev").unwrap();
        assert!(!b64.is_empty());
        // Verify it's valid base64 that decodes to a binary plist
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .expect("should be valid base64");
        assert!(decoded.len() > 50, "binary plist should be substantial");
        // Binary plist magic: "bplist"
        assert_eq!(&decoded[..6], b"bplist", "should be a binary plist");
    }

    #[test]
    fn skip_setup_assistant_creates_marker() {
        let tmp = tempfile::tempdir().unwrap();
        skip_setup_assistant(tmp.path()).unwrap();
        assert!(tmp.path().join("private/var/db/.AppleSetupDone").exists());
    }

    #[test]
    fn install_guest_agent_missing_binary() {
        let tmp = tempfile::tempdir().unwrap();
        let config = UserConfig::default();
        let result = install_guest_agent(
            tmp.path(),
            Path::new("/nonexistent/vz-guest-agent"),
            &config,
            AgentInstallMode::SystemLaunchDaemon,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn install_guest_agent_copies_files() {
        let tmp = tempfile::tempdir().unwrap();

        // Create a fake agent binary
        let fake_binary = tmp.path().join("fake-agent");
        std::fs::write(&fake_binary, b"#!/bin/bash\necho agent").unwrap();

        // Create a mount point
        let mount = tmp.path().join("mount");
        std::fs::create_dir_all(&mount).unwrap();

        let config = UserConfig::default();
        install_guest_agent(
            &mount,
            &fake_binary,
            &config,
            AgentInstallMode::SystemLaunchDaemon,
        )
        .unwrap();

        // Check binary was copied
        assert!(mount.join("usr/local/bin/vz-guest-agent").exists());

        // Check plist was written
        let plist = mount.join("Library/LaunchDaemons/com.vz.guest-agent.plist");
        assert!(plist.exists());
        let content = std::fs::read_to_string(plist).unwrap();
        assert!(content.contains("com.vz.guest-agent"));
    }

    #[test]
    fn install_guest_agent_user_mode_writes_launch_agent_files() {
        let tmp = tempfile::tempdir().unwrap();

        let fake_binary = tmp.path().join("fake-agent");
        std::fs::write(&fake_binary, b"#!/bin/bash\necho agent").unwrap();

        let mount = tmp.path().join("mount");
        std::fs::create_dir_all(&mount).unwrap();

        let config = UserConfig {
            username: "dev".to_string(),
            home: "/Users/dev".to_string(),
            ..Default::default()
        };
        install_guest_agent(
            &mount,
            &fake_binary,
            &config,
            AgentInstallMode::UserLaunchAgent,
        )
        .unwrap();

        let user_binary = mount.join("Users/dev/Library/Application Support/vz/vz-guest-agent");
        assert!(user_binary.exists());

        let plist = mount.join("Users/dev/Library/LaunchAgents/com.vz.user-guest-agent.plist");
        assert!(plist.exists());
        let content = std::fs::read_to_string(plist).unwrap();
        assert!(content.contains("com.vz.user-guest-agent"));
        assert!(content.contains("/Users/dev/Library/Application Support/vz/vz-guest-agent"));
        assert!(!content.contains(&mount.to_string_lossy().to_string()));
    }
}
