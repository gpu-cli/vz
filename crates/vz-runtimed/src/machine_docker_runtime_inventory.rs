//! Measured startup inventory from the original leased Machine, not Engine
//! runtime-name metadata. Full release-wide cache/execution inventory is separate.
use anyhow::{Result, ensure};
use serde::Serialize;
use std::time::Duration;
use vz_runtime_contract::{MachineIncarnation, ResourceOwner};

use crate::machine_runtime_activation::MachineRuntimeActivation;

#[derive(Debug, Clone, Serialize)]
pub struct VerifiedMachineRuntimeInventory {
    owner: ResourceOwner,
    incarnation: MachineIncarnation,
    youki_sha256: String,
    scope: &'static str,
    stdout: String,
}

impl VerifiedMachineRuntimeInventory {
    pub fn owner(&self) -> &ResourceOwner {
        &self.owner
    }
    pub fn incarnation(&self) -> &MachineIncarnation {
        &self.incarnation
    }
    pub fn youki_sha256(&self) -> &str {
        &self.youki_sha256
    }
    pub fn stdout(&self) -> &str {
        &self.stdout
    }

    pub async fn measure(
        activation: &MachineRuntimeActivation,
        incarnation: &MachineIncarnation,
        expected_sha256: &str,
    ) -> Result<Self> {
        ensure!(
            activation.owner().machine_id.as_ref() == Some(&incarnation.machine_id),
            "runtime inventory incarnation has foreign owner"
        );
        ensure!(
            expected_sha256.len() == 64
                && expected_sha256
                    .bytes()
                    .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)),
            "invalid expected youki digest"
        );
        let output = activation
            .exec(
                "/bin/busybox".into(),
                vec![
                    "sh".into(),
                    "-c".into(),
                    INVENTORY.into(),
                    "vz-runtime-inventory".into(),
                    expected_sha256.into(),
                ],
                Duration::from_secs(10),
            )
            .await?;
        ensure!(
            output.exit_code == 0
                && output.stderr.is_empty()
                && output.stdout.starts_with(&format!(
                    "vz-startup-runtime-inventory-v1\nyouki-sha256={expected_sha256}\n"
                ))
                && output.stdout.len() <= 8192,
            "exact Machine youki inventory failed: exit={} stdout={} stderr={}",
            output.exit_code,
            output.stdout,
            output.stderr
        );
        Ok(Self {
            owner: activation.owner().clone(),
            incarnation: incarnation.clone(),
            youki_sha256: expected_sha256.into(),
            scope: "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
            stdout: output.stdout,
        })
    }
}

const INVENTORY: &str = r#"
set -eu
fail() { printf 'runtime inventory: %s\n' "$1" >&2; exit 1; }
require_mount() {
  measured=$(/bin/busybox awk -v path="$2" '$2 == path { print $1 " " $3; count++ } END { if (count != 1) exit 1 }' /proc/mounts) || fail "missing or duplicated mount $2"
  test "$measured" = "$1 virtiofs" || fail "wrong mount identity $2: $measured"
}
require_mount linux-bin /mnt/linux-bin
require_mount vz-docker-bin /mnt/vz-docker-bin
test -x /mnt/linux-bin/youki || fail 'pinned youki is not executable'
test ! -L /mnt/linux-bin/youki || fail 'pinned youki is a symlink'
measured=$(/bin/busybox sha256sum /mnt/linux-bin/youki) || fail 'cannot hash pinned youki'
test "${measured%% *}" = "$1" || fail 'pinned youki digest mismatch'
for runtime_path in /usr/local/bin/youki /run/vz-oci/bin/youki; do
  test -x "$runtime_path" && test ! -L "$runtime_path" || fail "installed youki is missing, linked or not executable: $runtime_path"
  installed=$(/bin/busybox sha256sum "$runtime_path") || fail "cannot hash installed youki: $runtime_path"
  test "${installed%% *}" = "$1" || fail "installed youki digest mismatch: $runtime_path"
done
# The agent runs inside the overlay chroot. Its runtime copies are above;
# the initial /mnt/oci-runtime-bin mount belongs to the outer init root.
for directory in /bin /sbin /usr /run/vz-oci/bin /mnt/linux-bin /mnt/vz-docker-bin; do
  test -d "$directory" || fail "inventory directory is missing: $directory"
done
alternates=$(/bin/busybox find -L /bin /sbin /usr /run/vz-oci/bin /mnt/linux-bin /mnt/vz-docker-bin \( -name runc -o -name crun -o -name runsc -o -name kata-runtime -o -name buildkit-runc \) -print)
test -z "$alternates" || fail "alternate runtime paths found: $alternates"
PATH=/mnt/vz-docker-bin:/mnt/linux-bin:/bin:/sbin:/usr/bin:/usr/sbin
export PATH
for runtime in runc crun runsc kata-runtime buildkit-runc; do
  if command -v "$runtime" >/dev/null 2>&1; then fail "alternate runtime resolves: $runtime"; fi
done
printf 'vz-startup-runtime-inventory-v1\nyouki-sha256=%s\n' "$1"
/mnt/linux-bin/youki --version
printf 'alternate-runtime-binaries=absent\n'
"#;
