use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use oci_spec::runtime::{
    Capability, LinuxCapabilitiesBuilder, LinuxCpuBuilder, LinuxNamespaceType,
    LinuxResourcesBuilder, Mount, MountBuilder, ProcessBuilder, RootBuilder, Spec, SpecBuilder,
    User, UserBuilder, VERSION,
};

use crate::error::LinuxNativeError;

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
    pub network_namespace_path: Option<String>,
    /// Share the host network namespace instead of creating a new one.
    pub share_host_network: bool,
    /// CPU quota in microseconds per `cpu_period`.
    pub cpu_quota: Option<i64>,
    /// CPU CFS period in microseconds (default: 100000 = 100ms).
    pub cpu_period: Option<u64>,
    /// Redirect container stdout/stderr to a log file.
    pub capture_logs: bool,
}

/// Log directory inside the container where stdout/stderr are redirected.
const CONTAINER_LOG_DIR: &str = "/var/log/vz-oci";

/// Log file path inside the container (stdout + stderr interleaved).
pub const CONTAINER_LOG_FILE: &str = "/var/log/vz-oci/output.log";

/// Write an OCI bundle directory (`config.json` + `rootfs` link).
///
/// On Linux native, rootfs is typically a local filesystem path. When `rootfs_dir`
/// is absolute, `root.path` is set directly. When relative, a symlink is created.
pub fn write_oci_bundle(
    bundle_dir: impl AsRef<Path>,
    rootfs_dir: impl AsRef<Path>,
    spec: BundleSpec,
) -> Result<(), LinuxNativeError> {
    let bundle_dir = bundle_dir.as_ref();
    let rootfs_dir = rootfs_dir.as_ref();

    fs::create_dir_all(bundle_dir)?;

    let rootfs_path_in_spec = if rootfs_dir.is_absolute() {
        rootfs_dir.to_string_lossy().into_owned()
    } else {
        let rootfs_path = bundle_dir.join(OCI_ROOTFS_DIRNAME);
        replace_rootfs_link(&rootfs_path, rootfs_dir)?;
        OCI_ROOTFS_DIRNAME.to_string()
    };

    let runtime_spec = build_runtime_spec(spec, &rootfs_path_in_spec)?;
    let config_path = bundle_dir.join(OCI_CONFIG_FILENAME);
    runtime_spec.save(&config_path)?;

    Ok(())
}

fn build_runtime_spec(spec: BundleSpec, rootfs_path: &str) -> Result<Spec, LinuxNativeError> {
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
        return Err(LinuxNativeError::InvalidConfig(
            "run command must not be empty".to_string(),
        ));
    }

    let process_args = if capture_logs {
        wrap_cmd_with_log_redirect(&cmd)
    } else {
        cmd
    };

    let process = ProcessBuilder::default()
        .args(process_args)
        .env(
            env.into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>(),
        )
        .cwd(cwd.unwrap_or_else(|| "/".to_string()))
        .user(parse_process_user(user.as_deref())?)
        .capabilities(docker_default_capabilities()?)
        .build()?;

    sort_bundle_mounts(&mut mounts);
    let user_mounts = mounts
        .into_iter()
        .map(to_runtime_mount)
        .collect::<Result<Vec<_>, LinuxNativeError>>()?;

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

fn remove_network_namespace(spec: &mut Spec) {
    let Some(linux) = spec.linux_mut() else {
        return;
    };
    let Some(namespaces) = linux.namespaces_mut() else {
        return;
    };
    namespaces.retain(|ns| ns.typ() != LinuxNamespaceType::Network);
}

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

fn set_cpu_limits(
    spec: &mut Spec,
    quota: Option<i64>,
    period: Option<u64>,
) -> Result<(), LinuxNativeError> {
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

fn to_runtime_mount(mount: BundleMount) -> Result<Mount, LinuxNativeError> {
    if !mount.destination.is_absolute() {
        return Err(LinuxNativeError::InvalidConfig(format!(
            "mount destination must be absolute: {}",
            mount.destination.display()
        )));
    }

    if mount.typ.trim().is_empty() {
        return Err(LinuxNativeError::InvalidConfig(format!(
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
fn default_linux_mounts() -> Result<Vec<Mount>, LinuxNativeError> {
    Ok(vec![
        MountBuilder::default()
            .destination("/proc")
            .typ("proc")
            .source("proc")
            .options(vec!["nosuid".into(), "noexec".into(), "nodev".into()])
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
            .options(vec!["nosuid".into(), "noexec".into(), "nodev".into()])
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
fn docker_default_capabilities() -> Result<oci_spec::runtime::LinuxCapabilities, LinuxNativeError> {
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

fn parse_process_user(user: Option<&str>) -> Result<User, LinuxNativeError> {
    let Some(raw) = user else {
        return Ok(User::default());
    };

    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(User::default());
    }

    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() > 2 {
        return Err(LinuxNativeError::InvalidConfig(format!(
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
                LinuxNativeError::InvalidConfig(format!(
                    "unsupported group value '{group}' in user spec '{raw}'; only numeric gid is supported"
                ))
            })?;

            builder = builder.gid(gid);
        }
    }

    builder.build().map_err(Into::into)
}

fn replace_rootfs_link(link_path: &Path, target: &Path) -> Result<(), LinuxNativeError> {
    match fs::symlink_metadata(link_path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_dir() && !file_type.is_symlink() {
                fs::remove_dir_all(link_path)?;
            } else {
                fs::remove_file(link_path)?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    std::os::unix::fs::symlink(target, link_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut base = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        base.push(format!(
            "vz-linux-native-bundle-test-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn minimal_spec(cmd: Vec<String>) -> BundleSpec {
        BundleSpec {
            cmd,
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
        }
    }

    #[test]
    fn write_oci_bundle_generates_config_json() {
        let temp = unique_temp_dir("basic");
        let rootfs_source = temp.join("rootfs-source");
        fs::create_dir_all(&rootfs_source).unwrap();

        let bundle_dir = temp.join("bundle");
        write_oci_bundle(
            &bundle_dir,
            &rootfs_source,
            BundleSpec {
                cmd: vec!["/bin/echo".into(), "hello".into()],
                env: vec![("PATH".into(), "/usr/bin".into())],
                cwd: Some("/workspace".into()),
                user: Some("1000:1001".into()),
                mounts: vec![BundleMount {
                    destination: PathBuf::from("/data"),
                    source: PathBuf::from("/host/data"),
                    typ: "bind".into(),
                    options: vec!["rbind".into(), "rw".into()],
                }],
                oci_annotations: vec![("com.example.key".into(), "val".into())],
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

        let spec = Spec::load(&config_path).unwrap();
        let process = spec.process().as_ref().expect("process");

        assert_eq!(
            process.args().as_ref().expect("args"),
            &vec!["/bin/echo".to_string(), "hello".to_string()]
        );
        assert_eq!(process.user().uid(), 1000);
        assert_eq!(process.user().gid(), 1001);

        let mounts = spec.mounts().as_ref().expect("mounts");
        // 7 default + 1 user
        assert_eq!(mounts.len(), 8);
        assert_eq!(mounts[7].destination(), &PathBuf::from("/data"));
    }

    #[test]
    fn empty_cmd_is_rejected() {
        let temp = unique_temp_dir("empty-cmd");
        let rootfs = temp.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let result = write_oci_bundle(temp.join("bundle"), &rootfs, minimal_spec(Vec::new()));
        assert!(result.is_err());
    }

    #[test]
    fn network_namespace_path_is_set() {
        let temp = unique_temp_dir("netns");
        let rootfs = temp.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let mut spec = minimal_spec(vec!["/bin/sh".into()]);
        spec.network_namespace_path = Some("/var/run/netns/test".into());

        write_oci_bundle(temp.join("bundle"), &rootfs, spec).unwrap();

        let loaded = Spec::load(temp.join("bundle").join(OCI_CONFIG_FILENAME)).unwrap();
        let linux = loaded.linux().as_ref().expect("linux");
        let ns = linux.namespaces().as_ref().expect("namespaces");
        let netns = ns
            .iter()
            .find(|n| n.typ() == LinuxNamespaceType::Network)
            .expect("network ns");
        assert_eq!(
            netns.path().as_deref(),
            Some(Path::new("/var/run/netns/test"))
        );
    }

    #[test]
    fn share_host_network_removes_netns() {
        let temp = unique_temp_dir("host-net");
        let rootfs = temp.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let mut spec = minimal_spec(vec!["/bin/sh".into()]);
        spec.share_host_network = true;

        write_oci_bundle(temp.join("bundle"), &rootfs, spec).unwrap();

        let loaded = Spec::load(temp.join("bundle").join(OCI_CONFIG_FILENAME)).unwrap();
        let linux = loaded.linux().as_ref().expect("linux");
        let ns = linux.namespaces().as_ref().expect("namespaces");
        let netns = ns.iter().find(|n| n.typ() == LinuxNamespaceType::Network);
        assert!(netns.is_none());
    }

    #[test]
    fn cpu_limits_are_applied() {
        let temp = unique_temp_dir("cpu-limits");
        let rootfs = temp.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let mut spec = minimal_spec(vec!["/bin/sh".into()]);
        spec.cpu_quota = Some(50000);
        spec.cpu_period = Some(100000);

        write_oci_bundle(temp.join("bundle"), &rootfs, spec).unwrap();

        let loaded = Spec::load(temp.join("bundle").join(OCI_CONFIG_FILENAME)).unwrap();
        let linux = loaded.linux().as_ref().expect("linux");
        let resources = linux.resources().as_ref().expect("resources");
        let cpu = resources.cpu().as_ref().expect("cpu");
        assert_eq!(cpu.quota(), Some(50000));
        assert_eq!(cpu.period(), Some(100000));
    }

    #[test]
    fn capture_logs_wraps_command() {
        let temp = unique_temp_dir("capture-logs");
        let rootfs = temp.join("rootfs");
        fs::create_dir_all(&rootfs).unwrap();

        let mut spec = minimal_spec(vec![
            "redis-server".into(),
            "--appendonly".into(),
            "yes".into(),
        ]);
        spec.capture_logs = true;

        write_oci_bundle(temp.join("bundle"), &rootfs, spec).unwrap();

        let loaded = Spec::load(temp.join("bundle").join(OCI_CONFIG_FILENAME)).unwrap();
        let process = loaded.process().as_ref().expect("process");
        let args = process.args().as_ref().expect("args");
        assert_eq!(args[0], "/bin/sh");
        assert_eq!(args[1], "-c");
        assert!(args[2].contains("exec redis-server --appendonly yes"));
        assert!(args[2].contains(CONTAINER_LOG_FILE));
    }
}
