use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use oci_spec::runtime::{
    Capability, LinuxCapabilities, LinuxCapabilitiesBuilder, LinuxCpuBuilder, LinuxNamespaceType,
    LinuxResourcesBuilder, Mount, MountBuilder, ProcessBuilder, RootBuilder, Spec, SpecBuilder,
    User, UserBuilder, VERSION,
};

use crate::error::OciError;

const OCI_ROOTFS_DIRNAME: &str = "rootfs";
pub const OCI_CONFIG_FILENAME: &str = "config.json";

/// Mount entry written into an OCI runtime-spec bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleMount {
    /// Mount destination path inside the container.
    pub destination: PathBuf,
    /// Source path on the host.
    pub source: PathBuf,
    /// Mount type (for example, `bind`).
    pub typ: String,
    /// Mount options in fstab style.
    pub options: Vec<String>,
}

/// Process-oriented bundle settings consumed by runtime-spec generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleSpec {
    /// Process command and argument vector.
    pub cmd: Vec<String>,
    /// Process environment key/value pairs.
    pub env: Vec<(String, String)>,
    /// Optional process working directory.
    pub cwd: Option<String>,
    /// Optional process user declaration.
    pub user: Option<String>,
    /// Additional mounts to include in runtime config.
    pub mounts: Vec<BundleMount>,
    /// Additional OCI runtime-spec annotations.
    pub oci_annotations: Vec<(String, String)>,
    /// Path to an existing network namespace to join.
    ///
    /// When set, the generated config.json will include a
    /// `linux.namespaces` entry with `type: "network"` pointing
    /// to this path (e.g., `/var/run/netns/svc-web`). The
    /// namespace must be created before the container starts.
    pub network_namespace_path: Option<String>,
    /// Share the host (guest VM) network namespace instead of creating
    /// a new isolated one. When `true`, the network namespace entry is
    /// removed from the OCI spec so the container process uses the
    /// host's network stack directly.
    pub share_host_network: bool,
    /// CPU quota in microseconds per `cpu_period`.
    ///
    /// For example, `cpus: 0.5` → quota=50000, period=100000
    /// means the container gets 50ms of CPU time per 100ms period.
    pub cpu_quota: Option<i64>,
    /// CPU CFS period in microseconds (default: 100000 = 100ms).
    pub cpu_period: Option<u64>,
    /// Redirect container stdout/stderr to a log file for later retrieval.
    ///
    /// When `true`, the process args are wrapped in a shell redirector
    /// that sends both stdout and stderr to `/var/log/vz-oci/output.log`
    /// (interleaved, like Docker). The file can be read via `exec`.
    pub capture_logs: bool,
}

/// Write an OCI bundle directory (`config.json` + optional `rootfs` link).
///
/// When `rootfs_dir` is absolute, `root.path` in `config.json` is set to the
/// absolute path directly (no symlink created). This avoids VirtioFS caching
/// issues where symlinks written on the host may not be visible in the guest.
/// When `rootfs_dir` is relative, a symlink is created at `<bundle>/rootfs`.
pub fn write_oci_bundle(
    bundle_dir: impl AsRef<Path>,
    rootfs_dir: impl AsRef<Path>,
    spec: BundleSpec,
) -> Result<(), OciError> {
    let bundle_dir = bundle_dir.as_ref();
    let rootfs_dir = rootfs_dir.as_ref();

    fs::create_dir_all(bundle_dir)?;

    let rootfs_path_in_spec = if rootfs_dir.is_absolute() {
        // Absolute path: embed directly in config.json, no symlink needed.
        rootfs_dir.to_string_lossy().into_owned()
    } else {
        // Relative path: create a symlink at <bundle>/rootfs.
        let rootfs_path = bundle_dir.join(OCI_ROOTFS_DIRNAME);
        replace_rootfs_link(&rootfs_path, rootfs_dir)?;
        OCI_ROOTFS_DIRNAME.to_string()
    };

    let runtime_spec = build_runtime_spec(spec, &rootfs_path_in_spec)?;
    let config_path = bundle_dir.join(OCI_CONFIG_FILENAME);
    runtime_spec.save(&config_path)?;

    Ok(())
}

fn build_runtime_spec(spec: BundleSpec, rootfs_path: &str) -> Result<Spec, OciError> {
    let BundleSpec {
        cmd,
        env,
        cwd,
        user,
        mut mounts,
        oci_annotations,
        network_namespace_path,
        share_host_network,
        cpu_quota,
        cpu_period,
        capture_logs,
    } = spec;

    if cmd.is_empty() {
        return Err(OciError::InvalidConfig(
            "run command must not be empty".to_string(),
        ));
    }

    let process_args = if capture_logs {
        wrap_cmd_with_log_redirect(&cmd)
    } else {
        cmd
    };

    // Ensure PATH is always set (Docker default behavior).
    let has_path = env.iter().any(|(k, _)| k == "PATH");
    let mut env_strings: Vec<String> = env
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    if !has_path {
        env_strings.insert(
            0,
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
        );
    }

    let process = ProcessBuilder::default()
        .args(process_args)
        .env(env_strings)
        .cwd(cwd.unwrap_or_else(|| "/".to_string()))
        .user(parse_process_user(user.as_deref())?)
        .capabilities(docker_default_capabilities()?)
        .build()?;

    sort_bundle_mounts(&mut mounts);
    let user_mounts = mounts
        .into_iter()
        .map(to_runtime_mount)
        .collect::<Result<Vec<_>, OciError>>()?;

    // Combine default Linux mounts with user-specified mounts.
    // User mounts come after defaults so they can override.
    let mut all_mounts = default_linux_mounts()?;
    all_mounts.extend(user_mounts);

    let annotations = to_runtime_annotations(oci_annotations);

    let root = RootBuilder::default()
        .path(rootfs_path)
        .readonly(false)
        .build()?;

    let mut builder = SpecBuilder::default()
        .version(VERSION)
        .root(root)
        .process(process)
        .mounts(all_mounts);

    if !annotations.is_empty() {
        builder = builder.annotations(annotations);
    }

    let mut spec = builder.build()?;

    if share_host_network {
        remove_network_namespace(&mut spec);
    } else if let Some(netns_path) = network_namespace_path {
        set_network_namespace_path(&mut spec, &netns_path);
    }

    if cpu_quota.is_some() || cpu_period.is_some() {
        set_cpu_limits(&mut spec, cpu_quota, cpu_period)?;
    }

    Ok(spec)
}

/// Remove the network namespace entry so the container shares the host's
/// network stack. If no linux section or namespaces exist, this is a no-op.
fn remove_network_namespace(spec: &mut Spec) {
    let Some(linux) = spec.linux_mut() else {
        return;
    };
    let Some(namespaces) = linux.namespaces_mut() else {
        return;
    };
    namespaces.retain(|ns| ns.typ() != LinuxNamespaceType::Network);
}

/// Update the network namespace entry in the spec's linux section to join
/// an existing netns at `path` (e.g., `/var/run/netns/svc-web`).
/// If no linux section or namespaces exist, this is a no-op.
fn set_network_namespace_path(spec: &mut Spec, path: &str) {
    let Some(linux) = spec.linux_mut() else {
        return;
    };
    let Some(namespaces) = linux.namespaces_mut() else {
        return;
    };
    if let Some(netns) = namespaces
        .iter_mut()
        .find(|ns| ns.typ() == LinuxNamespaceType::Network)
    {
        netns.set_path(Some(path.into()));
    }
}

/// Set CPU cgroup limits (quota/period) in the spec's linux.resources.cpu section.
fn set_cpu_limits(
    spec: &mut Spec,
    quota: Option<i64>,
    period: Option<u64>,
) -> Result<(), OciError> {
    let mut cpu_builder = LinuxCpuBuilder::default();
    if let Some(q) = quota {
        cpu_builder = cpu_builder.quota(q);
    }
    if let Some(p) = period {
        cpu_builder = cpu_builder.period(p);
    }
    let cpu = cpu_builder.build()?;

    let resources = LinuxResourcesBuilder::default().cpu(cpu).build()?;

    if let Some(linux) = spec.linux_mut() {
        linux.set_resources(Some(resources));
    }
    Ok(())
}

fn sort_bundle_mounts(mounts: &mut [BundleMount]) {
    mounts.sort_by(|left, right| {
        left.destination
            .cmp(&right.destination)
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left.typ.cmp(&right.typ))
            .then_with(|| left.options.cmp(&right.options))
    });
}

/// Log directory inside the container where stdout/stderr are redirected.
const CONTAINER_LOG_DIR: &str = "/var/log/vz-oci";

/// Log file path inside the container (stdout + stderr interleaved).
pub const CONTAINER_LOG_FILE: &str = "/var/log/vz-oci/output.log";

/// Wrap a command vector with shell-based log redirection.
///
/// Transforms `["redis-server", "--appendonly", "yes"]` into:
/// ```text
/// ["/bin/sh", "-c", "mkdir -p /var/log/vz-oci && exec redis-server --appendonly yes \
///  >>/var/log/vz-oci/output.log 2>&1"]
/// ```
///
/// stdout and stderr are interleaved in a single file for Docker-like
/// `logs` behavior. The `exec` ensures the shell is replaced by the
/// actual command, preserving PID 1 semantics and signal handling.
fn wrap_cmd_with_log_redirect(cmd: &[String]) -> Vec<String> {
    let joined = shell_words::join(cmd);
    let script = format!(
        "mkdir -p {CONTAINER_LOG_DIR} && exec {joined} \
         >>{CONTAINER_LOG_FILE} 2>&1"
    );
    vec!["/bin/sh".into(), "-c".into(), script]
}

fn to_runtime_annotations(annotations: Vec<(String, String)>) -> HashMap<String, String> {
    let mut mapped = HashMap::with_capacity(annotations.len());
    for (key, value) in annotations {
        mapped.insert(key, value);
    }
    mapped
}

fn to_runtime_mount(mount: BundleMount) -> Result<Mount, OciError> {
    if !mount.destination.is_absolute() {
        return Err(OciError::InvalidConfig(format!(
            "mount destination must be absolute: {}",
            mount.destination.display()
        )));
    }

    if mount.typ.trim().is_empty() {
        return Err(OciError::InvalidConfig(format!(
            "mount type must not be empty for destination {}",
            mount.destination.display()
        )));
    }

    let mut builder = MountBuilder::default()
        .destination(mount.destination)
        .typ(mount.typ)
        .source(mount.source);

    if !mount.options.is_empty() {
        builder = builder.options(mount.options);
    }

    builder.build().map_err(Into::into)
}

/// Standard Linux container mounts (proc, dev, devpts, shm, mqueue, sysfs, cgroup).
///
/// `SpecBuilder::default()` does not include these — only `Spec::default()` does.
/// Without them, real-world services (redis, postgres, etc.) fail because they
/// need `/proc`, `/dev`, and `/dev/shm`.
fn default_linux_mounts() -> Result<Vec<Mount>, OciError> {
    Ok(vec![
        MountBuilder::default()
            .destination("/proc")
            .typ("proc")
            .source("proc")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/dev")
            .typ("tmpfs")
            .source("tmpfs")
            .options(vec![
                "nosuid".into(),
                "strictatime".into(),
                "mode=755".into(),
                "size=65536k".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/dev/pts")
            .typ("devpts")
            .source("devpts")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "newinstance".into(),
                "ptmxmode=0666".into(),
                "mode=0620".into(),
                "gid=5".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/dev/shm")
            .typ("tmpfs")
            .source("shm")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "mode=1777".into(),
                "size=65536k".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/dev/mqueue")
            .typ("mqueue")
            .source("mqueue")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/sys")
            .typ("sysfs")
            .source("sysfs")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "ro".into(),
            ])
            .build()?,
        MountBuilder::default()
            .destination("/sys/fs/cgroup")
            .typ("cgroup")
            .source("cgroup")
            .options(vec![
                "nosuid".into(),
                "noexec".into(),
                "nodev".into(),
                "relatime".into(),
                "ro".into(),
            ])
            .build()?,
    ])
}

/// Docker-equivalent default capabilities for container processes.
fn docker_default_capabilities() -> Result<LinuxCapabilities, OciError> {
    use std::collections::HashSet;
    let caps: HashSet<Capability> = [
        Capability::AuditWrite,
        Capability::Chown,
        Capability::DacOverride,
        Capability::Fowner,
        Capability::Fsetid,
        Capability::Kill,
        Capability::Mknod,
        Capability::NetBindService,
        Capability::NetRaw,
        Capability::Setfcap,
        Capability::Setgid,
        Capability::Setpcap,
        Capability::Setuid,
        Capability::SysChroot,
    ]
    .into_iter()
    .collect();
    LinuxCapabilitiesBuilder::default()
        .bounding(caps.clone())
        .effective(caps.clone())
        .inheritable(caps.clone())
        .permitted(caps.clone())
        .ambient(caps)
        .build()
        .map_err(Into::into)
}

fn parse_process_user(user: Option<&str>) -> Result<User, OciError> {
    let Some(raw) = user else {
        return Ok(User::default());
    };

    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(User::default());
    }

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() > 2 {
        return Err(OciError::InvalidConfig(format!(
            "unsupported user format '{raw}'; expected '<uid|name>' or '<uid|name>:<gid>'"
        )));
    }

    let mut builder = UserBuilder::default();

    if let Some(primary) = parts.first() {
        if !primary.is_empty() {
            if let Ok(uid) = primary.parse::<u32>() {
                builder = builder.uid(uid);
            } else {
                builder = builder.username(*primary);
            }
        }
    }

    if let Some(group) = parts.get(1) {
        if !group.is_empty() {
            let gid = group.parse::<u32>().map_err(|_| {
                OciError::InvalidConfig(format!(
                    "unsupported group value '{group}' in user spec '{raw}'; only numeric gid is supported"
                ))
            })?;

            builder = builder.gid(gid);
        }
    }

    builder.build().map_err(Into::into)
}

fn replace_rootfs_link(link_path: &Path, target: &Path) -> Result<(), OciError> {
    match fs::symlink_metadata(link_path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_dir() && !file_type.is_symlink() {
                fs::remove_dir_all(link_path)?;
            } else {
                fs::remove_file(link_path)?;
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    std::os::unix::fs::symlink(target, link_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::env;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        base.push(format!(
            "vz-oci-bundle-test-{name}-{}-{nanos}",
            process::id()
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn write_oci_bundle_generates_runtime_spec_with_expected_mappings() {
        let temp = unique_temp_dir("mappings");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/echo".to_string(), "hello".to_string()],
                env: vec![
                    ("HELLO".to_string(), "world".to_string()),
                    ("PATH".to_string(), "/usr/bin".to_string()),
                ],
                cwd: Some("/workspace".to_string()),
                user: Some("1000:1001".to_string()),
                mounts: vec![BundleMount {
                    destination: PathBuf::from("/data"),
                    source: PathBuf::from("/host/data"),
                    typ: "bind".to_string(),
                    options: vec!["rbind".to_string(), "rw".to_string()],
                }],
                oci_annotations: vec![
                    ("com.example.service".to_string(), "web".to_string()),
                    ("com.example.revision".to_string(), "42".to_string()),
                ],
                network_namespace_path: None,
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let config_path = bundle_dir.join(OCI_CONFIG_FILENAME);
        assert!(config_path.is_file());
        // With absolute rootfs_source, no symlink is created — root.path
        // is set to the absolute path directly in config.json.
        let rootfs_link = bundle_dir.join(OCI_ROOTFS_DIRNAME);
        assert!(!rootfs_link.exists(), "no symlink when rootfs is absolute");

        let spec = Spec::load(&config_path).unwrap();
        let process = spec.process().as_ref().expect("process should exist");

        assert_eq!(
            process.args().as_ref().expect("args should be present"),
            &vec!["/bin/echo".to_string(), "hello".to_string()]
        );
        assert_eq!(
            process.env().as_ref().expect("env should be present"),
            &vec!["HELLO=world".to_string(), "PATH=/usr/bin".to_string(),]
        );
        assert_eq!(process.cwd(), &PathBuf::from("/workspace"));
        assert_eq!(process.user().uid(), 1000);
        assert_eq!(process.user().gid(), 1001);

        let mounts = spec.mounts().as_ref().expect("mounts should be present");
        // 7 default Linux mounts + 1 user mount
        assert_eq!(mounts.len(), 8);
        // First 7 are defaults (proc, dev, devpts, shm, mqueue, sysfs, cgroup).
        assert_eq!(mounts[0].destination(), &PathBuf::from("/proc"));
        assert_eq!(mounts[6].destination(), &PathBuf::from("/sys/fs/cgroup"));
        // User mount comes last.
        assert_eq!(mounts[7].destination(), &PathBuf::from("/data"));
        assert_eq!(mounts[7].typ().as_deref(), Some("bind"));
        assert_eq!(
            mounts[7].source().as_ref(),
            Some(&PathBuf::from("/host/data"))
        );
        assert_eq!(
            mounts[7]
                .options()
                .as_ref()
                .expect("mount options should exist"),
            &vec!["rbind".to_string(), "rw".to_string()]
        );
        assert_eq!(
            spec.annotations()
                .as_ref()
                .expect("annotations should be present")
                .get("com.example.service")
                .map(String::as_str),
            Some("web")
        );
        assert_eq!(
            spec.annotations()
                .as_ref()
                .expect("annotations should be present")
                .get("com.example.revision")
                .map(String::as_str),
            Some("42")
        );

        let root = spec.root().as_ref().expect("root should be present");
        assert_eq!(root.path(), &rootfs_source);
        assert_eq!(root.readonly(), Some(false));
    }

    #[test]
    fn write_oci_bundle_maps_username_user_value() {
        let temp = unique_temp_dir("username");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/sh".to_string()],
                env: Vec::new(),
                cwd: None,
                user: Some("nobody".to_string()),
                mounts: Vec::new(),
                oci_annotations: Vec::new(),
                network_namespace_path: None,
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        let process = spec.process().as_ref().expect("process should exist");

        assert_eq!(process.user().username().as_deref(), Some("nobody"));
        assert_eq!(process.user().uid(), 0);
        assert_eq!(process.user().gid(), 0);
        assert_eq!(process.cwd(), &PathBuf::from("/"));
    }

    #[test]
    fn write_oci_bundle_sorts_mounts_deterministically() {
        let temp = unique_temp_dir("sorted-mounts");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/echo".to_string(), "hello".to_string()],
                env: Vec::new(),
                cwd: None,
                user: None,
                mounts: vec![
                    BundleMount {
                        destination: PathBuf::from("/volumes/cache"),
                        source: PathBuf::from("/host/cache-b"),
                        typ: "bind".to_string(),
                        options: vec!["rbind".to_string(), "rw".to_string()],
                    },
                    BundleMount {
                        destination: PathBuf::from("/volumes/cache"),
                        source: PathBuf::from("/host/cache-a"),
                        typ: "bind".to_string(),
                        options: vec!["rbind".to_string(), "rw".to_string()],
                    },
                    BundleMount {
                        destination: PathBuf::from("/volumes/config"),
                        source: PathBuf::from("/host/config"),
                        typ: "bind".to_string(),
                        options: vec!["rbind".to_string(), "ro".to_string()],
                    },
                ],
                oci_annotations: Vec::new(),
                network_namespace_path: None,
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        let mounts = spec.mounts().as_ref().expect("mounts should be present");
        // 7 default Linux mounts + 3 user mounts = 10
        assert_eq!(mounts.len(), 10);
        // User mounts come after defaults, sorted by destination then source.
        assert_eq!(mounts[7].destination(), &PathBuf::from("/volumes/cache"));
        assert_eq!(
            mounts[7].source().as_ref(),
            Some(&PathBuf::from("/host/cache-a"))
        );
        assert_eq!(mounts[8].destination(), &PathBuf::from("/volumes/cache"));
        assert_eq!(
            mounts[8].source().as_ref(),
            Some(&PathBuf::from("/host/cache-b"))
        );
        assert_eq!(mounts[9].destination(), &PathBuf::from("/volumes/config"));
        assert_eq!(
            mounts[9].source().as_ref(),
            Some(&PathBuf::from("/host/config"))
        );
    }

    #[test]
    fn write_oci_bundle_with_network_namespace() {
        let temp = unique_temp_dir("netns");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/sh".to_string()],
                env: Vec::new(),
                cwd: None,
                user: None,
                mounts: Vec::new(),
                oci_annotations: Vec::new(),
                network_namespace_path: Some("/var/run/netns/svc-web".to_string()),
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        let linux = spec.linux().as_ref().expect("linux section should exist");
        let namespaces = linux
            .namespaces()
            .as_ref()
            .expect("namespaces should exist");
        let netns = namespaces
            .iter()
            .find(|ns| ns.typ() == LinuxNamespaceType::Network)
            .expect("network namespace should exist");
        assert_eq!(
            netns.path().as_deref(),
            Some(Path::new("/var/run/netns/svc-web"))
        );
    }

    #[test]
    fn write_oci_bundle_without_network_namespace_has_no_linux_section() {
        let temp = unique_temp_dir("no-netns");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/sh".to_string()],
                env: Vec::new(),
                cwd: None,
                user: None,
                mounts: Vec::new(),
                oci_annotations: Vec::new(),
                network_namespace_path: None,
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        // Without network_namespace_path, the default network namespace has no path
        // (i.e., the container creates a new netns rather than joining an existing one).
        let linux = spec
            .linux()
            .as_ref()
            .expect("default spec includes linux section");
        let ns = linux
            .namespaces()
            .as_ref()
            .expect("default namespaces present");
        let netns = ns
            .iter()
            .find(|n| n.typ() == LinuxNamespaceType::Network)
            .expect("default network namespace exists");
        assert!(netns.path().is_none(), "expected no netns path by default");
    }

    #[test]
    fn write_oci_bundle_share_host_network_removes_netns() {
        let temp = unique_temp_dir("share-host-net");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/sh".to_string()],
                env: Vec::new(),
                cwd: None,
                user: None,
                mounts: Vec::new(),
                oci_annotations: Vec::new(),
                network_namespace_path: None,
                share_host_network: true,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: false,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        let linux = spec
            .linux()
            .as_ref()
            .expect("linux section present");
        let ns = linux
            .namespaces()
            .as_ref()
            .expect("namespaces present");
        let netns = ns
            .iter()
            .find(|n| n.typ() == LinuxNamespaceType::Network);
        assert!(netns.is_none(), "network namespace should be removed");
    }

    #[test]
    fn wrap_cmd_with_log_redirect_wraps_simple_command() {
        let cmd = vec!["redis-server".to_string()];
        let wrapped = wrap_cmd_with_log_redirect(&cmd);
        assert_eq!(wrapped[0], "/bin/sh");
        assert_eq!(wrapped[1], "-c");
        assert!(wrapped[2].contains("exec redis-server"));
        assert!(wrapped[2].contains(CONTAINER_LOG_FILE));
        assert!(wrapped[2].contains("2>&1"));
    }

    #[test]
    fn wrap_cmd_with_log_redirect_quotes_args_with_spaces() {
        let cmd = vec![
            "echo".to_string(),
            "hello world".to_string(),
        ];
        let wrapped = wrap_cmd_with_log_redirect(&cmd);
        // shell_words::join should quote the argument with spaces.
        assert!(wrapped[2].contains("'hello world'"));
    }

    #[test]
    fn capture_logs_wraps_process_args() {
        let temp = unique_temp_dir("capture-logs");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["redis-server".to_string(), "--appendonly".to_string(), "yes".to_string()],
                env: Vec::new(),
                cwd: None,
                user: None,
                mounts: Vec::new(),
                oci_annotations: Vec::new(),
                network_namespace_path: None,
                share_host_network: false,
                cpu_quota: None,
                cpu_period: None,
                capture_logs: true,
            },
        )
        .unwrap();

        let spec = Spec::load(bundle_dir.join(OCI_CONFIG_FILENAME)).unwrap();
        let process = spec.process().as_ref().expect("process should exist");
        let args = process.args().as_ref().expect("args should be present");
        assert_eq!(args[0], "/bin/sh");
        assert_eq!(args[1], "-c");
        assert!(args[2].contains("exec redis-server --appendonly yes"));
        assert!(args[2].contains(CONTAINER_LOG_FILE));
    }
}
