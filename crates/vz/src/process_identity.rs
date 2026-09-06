//! Read-only native macOS process birth observations.
//!
//! An observation is a point-in-time fact, not a signal handle or permission to
//! reclaim resources. Callers must retain independent ownership fences. Only an
//! explicit kernel ESRCH result means absence; permissions, short reads and
//! malformed identities remain errors. PID reuse is distinguished by birth time
//! and boot-session UUID, never by command names or PID-file contents.

use std::io;
use std::mem::{MaybeUninit, size_of};

use serde::{Deserialize, Serialize};

/// Exact host process birth within one boot session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub uid: u32,
    pub start_seconds: u64,
    pub start_microseconds: u64,
    pub boot_session_uuid: String,
}

/// A birth observation, retaining zombie state separately from its identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessObservation {
    pub identity: ProcessIdentity,
    pub zombie: bool,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn checked_pid(pid: u32) -> io::Result<libc::pid_t> {
    if pid == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PID zero is not a process identity",
        ));
    }
    libc::pid_t::try_from(pid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PID exceeds native process range",
        )
    })
}

/// Classify only the syscall's immediate return/errno pair. A partial positive
/// result is never an absence proof, even if errno happens to contain ESRCH.
fn complete_process_result(returned: libc::c_int, errno: libc::c_int) -> io::Result<bool> {
    if returned <= 0 {
        if errno == libc::ESRCH {
            return Ok(false);
        }
        return Err(if errno == 0 {
            io::Error::other("process identity lookup failed without an explicit errno")
        } else {
            io::Error::from_raw_os_error(errno)
        });
    }
    if usize::try_from(returned).ok() != Some(size_of::<libc::proc_bsdinfo>()) {
        return Err(invalid(
            "process identity lookup returned a partial or unexpected structure",
        ));
    }
    Ok(true)
}

fn parse_boot_session(bytes: &[u8]) -> io::Result<String> {
    // kern.bootsessionuuid is a NUL-terminated canonical UUID. Never accept a
    // truncated buffer, trailing bytes, interior NUL, or lossy text conversion.
    if bytes.len() != 37 || bytes[36] != 0 {
        return Err(invalid(
            "boot-session UUID has an unexpected length or terminator",
        ));
    }
    for (index, byte) in bytes[..36].iter().enumerate() {
        if if matches!(index, 8 | 13 | 18 | 23) {
            *byte != b'-'
        } else {
            !byte.is_ascii_hexdigit()
        } {
            return Err(invalid("boot-session UUID is not canonical UUID text"));
        }
    }
    let text =
        std::str::from_utf8(&bytes[..36]).map_err(|_| invalid("boot-session UUID is not ASCII"))?;
    Ok(text.to_ascii_lowercase())
}

fn boot_session_uuid() -> io::Result<String> {
    let mut bytes = [0_u8; 64];
    let mut length = bytes.len();
    // SAFETY: the sysctl name is static NUL-terminated text, bytes is writable
    // for the supplied length, and length points to a live size_t. Null newp
    // with zero newlen makes this read-only; no host settings are changed.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let observed = bytes
        .get(..length)
        .ok_or_else(|| invalid("boot-session UUID exceeded the fixed observation buffer"))?;
    parse_boot_session(observed)
}

fn validate_fields(pid: u32, uid: u32, info: &libc::proc_bsdinfo) -> io::Result<()> {
    if info.pbi_pid != pid {
        return Err(invalid("kernel process identity returned a different PID"));
    }
    if info.pbi_uid != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process identity is not owned by the effective user",
        ));
    }
    if info.pbi_start_tvsec == 0 || info.pbi_start_tvusec >= 1_000_000 {
        return Err(invalid(
            "kernel process identity returned an invalid birth timestamp",
        ));
    }
    Ok(())
}

/// Observe one exact same-euid process without changing or signalling it.
///
/// Returns `None` only for an explicit ESRCH from `proc_pidinfo`. A process can
/// exit immediately after a successful observation; consumers must not mistake
/// this snapshot for retained process ownership or a safe numeric-PID signal.
pub fn capture(pid: u32) -> io::Result<Option<ProcessObservation>> {
    let native_pid = checked_pid(pid)?;
    // SAFETY: geteuid has no pointer arguments or mutable process effects.
    let uid = unsafe { libc::geteuid() };
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let buffer_size = libc::c_int::try_from(size_of::<libc::proc_bsdinfo>())
        .map_err(|_| invalid("native process structure size exceeds libc argument range"))?;
    // SAFETY: __error returns this thread's live errno location. Clearing it
    // prevents a failed call which sets no errno from inheriting stale ESRCH.
    // proc_pidinfo receives a correctly aligned writable buffer of exactly its
    // declared size. No fields are read unless the full structure was returned.
    let (returned, errno) = unsafe {
        *libc::__error() = 0;
        let returned = libc::proc_pidinfo(
            native_pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            buffer_size,
        );
        (returned, *libc::__error())
    };
    if !complete_process_result(returned, errno)? {
        return Ok(None);
    }
    // SAFETY: proc_pidinfo returned precisely sizeof(proc_bsdinfo). The buffer
    // was zero-initialized as well, including any padding the kernel leaves.
    let info = unsafe { info.assume_init() };
    validate_fields(pid, uid, &info)?;
    Ok(Some(ProcessObservation {
        identity: ProcessIdentity {
            pid,
            uid,
            start_seconds: info.pbi_start_tvsec,
            start_microseconds: info.pbi_start_tvusec,
            boot_session_uuid: boot_session_uuid()?,
        },
        zombie: info.pbi_status == libc::SZOMB,
    }))
}

/// Capture the calling process's exact native birth identity.
pub fn current() -> io::Result<ProcessIdentity> {
    let observation = capture(std::process::id())?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "calling process identity was absent",
        )
    })?;
    if observation.zombie {
        return Err(invalid(
            "calling process was unexpectedly reported as a zombie",
        ));
    }
    Ok(observation.identity)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn current_identity_is_stable_and_serializes_exactly() -> io::Result<()> {
        let first = current()?;
        assert_eq!(first, current()?);
        assert_eq!(first.pid, std::process::id());
        let observation = capture(first.pid)?.expect("current process exists");
        assert_eq!(observation.identity, first);
        assert!(!observation.zombie);
        let json = serde_json::to_vec(&first).map_err(io::Error::other)?;
        assert_eq!(
            serde_json::from_slice::<ProcessIdentity>(&json).map_err(io::Error::other)?,
            first
        );
        Ok(())
    }

    #[test]
    fn zero_and_out_of_range_pids_reject_before_lookup() {
        for pid in [0, i32::MAX as u32 + 1, u32::MAX] {
            assert_eq!(
                capture(pid).expect_err("invalid process selector").kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert!(checked_pid(1).is_ok());
        assert!(checked_pid(i32::MAX as u32).is_ok());
    }

    #[test]
    fn only_explicit_esrch_is_absence_not_short_or_permission_failure() {
        assert!(!complete_process_result(0, libc::ESRCH).expect("explicit absence"));
        assert!(!complete_process_result(-1, libc::ESRCH).expect("explicit absence"));
        for errno in [0, libc::EPERM, libc::EACCES, libc::EIO, libc::EINVAL] {
            assert!(complete_process_result(0, errno).is_err());
        }
        let full = i32::try_from(size_of::<libc::proc_bsdinfo>()).expect("native size");
        for returned in [1, full - 1, full + 1] {
            assert!(complete_process_result(returned, libc::ESRCH).is_err());
        }
        assert!(complete_process_result(full, 0).expect("complete structure"));
    }

    #[test]
    fn boot_uuid_rejects_truncated_oversized_nonascii_and_embedded_nul() {
        let valid = b"31FD32E6-E95B-422C-973F-54C79DED35EA\0";
        assert_eq!(
            parse_boot_session(valid).expect("canonical UUID"),
            "31fd32e6-e95b-422c-973f-54c79ded35ea"
        );
        for length in [0, 1, 35, 36] {
            assert!(parse_boot_session(&valid[..length]).is_err());
        }
        let mut extra = valid.to_vec();
        extra.push(0);
        assert!(parse_boot_session(&extra).is_err());
        for (index, value) in [(0, 0), (0, 255), (8, b'a'), (36, b'x')] {
            let mut bad = *valid;
            bad[index] = value;
            assert!(parse_boot_session(&bad).is_err());
        }
    }

    #[test]
    fn kernel_fields_reject_foreign_pid_uid_and_invalid_birth() {
        // SAFETY: proc_bsdinfo contains only integer and fixed byte-array fields,
        // so an all-zero value is valid to initialize a pure validation fixture.
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        info.pbi_pid = 12;
        info.pbi_uid = 34;
        info.pbi_start_tvsec = 100;
        info.pbi_start_tvusec = 999_999;
        assert!(validate_fields(12, 34, &info).is_ok());
        assert!(validate_fields(13, 34, &info).is_err());
        assert_eq!(
            validate_fields(12, 35, &info)
                .expect_err("foreign owner")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        info.pbi_start_tvusec = 1_000_000;
        assert!(validate_fields(12, 34, &info).is_err());
        info.pbi_start_tvusec = 0;
        info.pbi_start_tvsec = 0;
        assert!(validate_fields(12, 34, &info).is_err());
    }

    #[test]
    fn owned_child_birth_then_reaped_absence() -> io::Result<()> {
        struct OwnedChild(std::process::Child);
        impl Drop for OwnedChild {
            fn drop(&mut self) {
                if self.0.try_wait().ok().flatten().is_none() {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
        }
        let mut child = OwnedChild(
            std::process::Command::new("/bin/sleep")
                .arg("30")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?,
        );
        let pid = child.0.id();
        let observed = capture(pid)?.expect("owned child exists");
        assert_eq!(observed.identity.pid, pid);
        assert!(!observed.zombie);
        assert_eq!(
            capture(pid)?.expect("same child").identity,
            observed.identity
        );
        child.0.kill()?;
        child.0.wait()?;
        assert!(
            capture(pid)?.is_none(),
            "reaped owned child must report explicit ESRCH"
        );
        Ok(())
    }
}
