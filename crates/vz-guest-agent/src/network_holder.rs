//! Portable lifecycle coordination for the Linux network-namespace holder.
//!
//! The Linux syscall adapter lives in `network.rs`. Keeping the orchestration
//! here lets host-side tests cover every post-fork parent path without creating
//! namespaces or processes.

use std::io::{self, Read};
use std::os::fd::RawFd;

pub(crate) trait NamespaceHolderOps {
    fn setns(&mut self, ns_fd: RawFd) -> bool;
    fn signal_ready(&mut self, ready_fd: RawFd) -> bool;
    fn move_link(&mut self, dev: &str, pid: libc::pid_t) -> io::Result<()>;
    fn terminate(&mut self, pid: libc::pid_t) -> io::Result<()>;
    fn reap(&mut self, pid: libc::pid_t) -> io::Result<()>;
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum HolderStartupFailure {
    Setns,
    Readiness,
}

impl HolderStartupFailure {
    pub(crate) const fn exit_code(&self) -> libc::c_int {
        match self {
            Self::Setns => 1,
            Self::Readiness => 2,
        }
    }
}

pub(crate) fn prepare_namespace_holder(
    ops: &mut impl NamespaceHolderOps,
    ns_fd: RawFd,
    ready_fd: RawFd,
) -> Result<(), HolderStartupFailure> {
    if !ops.setns(ns_fd) {
        return Err(HolderStartupFailure::Setns);
    }
    if !ops.signal_ready(ready_fd) {
        return Err(HolderStartupFailure::Readiness);
    }
    Ok(())
}

pub(crate) fn complete_namespace_handoff(
    ops: &mut impl NamespaceHolderOps,
    ready_reader: &mut impl Read,
    dev: &str,
    ns_path: &str,
    pid: libc::pid_t,
) -> io::Result<()> {
    let primary_result = wait_for_namespace_holder_ready(ready_reader)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("namespace holder for {ns_path} failed before handoff: {error}"),
            )
        })
        .and_then(|()| ops.move_link(dev, pid));

    // Once fork returns a child PID, both cleanup operations are mandatory on
    // readiness failure, handoff failure, and success. Reaping is still tried
    // when termination itself fails.
    let terminate_result = ops.terminate(pid);
    let reap_result = ops.reap(pid);

    combine_lifecycle_results(primary_result, terminate_result, reap_result, pid)
}

pub(crate) fn wait_for_namespace_holder_ready(reader: &mut impl Read) -> io::Result<()> {
    let mut ready = [0_u8; 1];
    reader.read_exact(&mut ready)?;
    if ready[0] != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected namespace readiness byte {}", ready[0]),
        ));
    }
    Ok(())
}

pub(crate) fn retry_waitpid(
    pid: libc::pid_t,
    mut waitpid: impl FnMut(libc::pid_t) -> io::Result<libc::pid_t>,
) -> io::Result<()> {
    loop {
        match waitpid(pid) {
            Ok(waited) if waited == pid => return Ok(()),
            Ok(waited) => {
                return Err(io::Error::other(format!(
                    "waitpid returned unexpected PID {waited} while reaping namespace holder {pid}"
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn combine_lifecycle_results(
    primary_result: io::Result<()>,
    terminate_result: io::Result<()>,
    reap_result: io::Result<()>,
    pid: libc::pid_t,
) -> io::Result<()> {
    let mut cleanup_failures = Vec::new();
    let mut cleanup_kind = None;

    if let Err(error) = terminate_result {
        cleanup_kind = Some(error.kind());
        cleanup_failures.push(format!("terminate failed: {error}"));
    }
    if let Err(error) = reap_result {
        cleanup_kind.get_or_insert(error.kind());
        cleanup_failures.push(format!("reap failed: {error}"));
    }

    match (primary_result, cleanup_failures.is_empty()) {
        (Ok(()), true) => Ok(()),
        (Ok(()), false) => Err(io::Error::new(
            cleanup_kind.unwrap_or(io::ErrorKind::Other),
            format!(
                "namespace holder {pid} cleanup failed: {}",
                cleanup_failures.join("; ")
            ),
        )),
        (Err(primary), true) => Err(primary),
        (Err(primary), false) => Err(io::Error::new(
            primary.kind(),
            format!(
                "{primary}; namespace holder {pid} cleanup also failed: {}",
                cleanup_failures.join("; ")
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_FD: RawFd = 11;
    const READY_FD: RawFd = 12;
    const PID: libc::pid_t = 42;

    #[derive(Debug, Eq, PartialEq)]
    enum Event {
        Setns(RawFd),
        SignalReady(RawFd),
        MoveLink(String, libc::pid_t),
        Terminate(libc::pid_t),
        Reap(libc::pid_t),
    }

    #[derive(Default)]
    struct FakeOps {
        events: Vec<Event>,
        setns_fails: bool,
        readiness_fails: bool,
        move_error: Option<io::ErrorKind>,
        terminate_error: Option<io::ErrorKind>,
        reap_error: Option<io::ErrorKind>,
    }

    impl NamespaceHolderOps for FakeOps {
        fn setns(&mut self, ns_fd: RawFd) -> bool {
            self.events.push(Event::Setns(ns_fd));
            !self.setns_fails
        }

        fn signal_ready(&mut self, ready_fd: RawFd) -> bool {
            self.events.push(Event::SignalReady(ready_fd));
            !self.readiness_fails
        }

        fn move_link(&mut self, dev: &str, pid: libc::pid_t) -> io::Result<()> {
            self.events.push(Event::MoveLink(dev.to_string(), pid));
            configured_result(self.move_error, "move failed")
        }

        fn terminate(&mut self, pid: libc::pid_t) -> io::Result<()> {
            self.events.push(Event::Terminate(pid));
            configured_result(self.terminate_error, "terminate failed")
        }

        fn reap(&mut self, pid: libc::pid_t) -> io::Result<()> {
            self.events.push(Event::Reap(pid));
            configured_result(self.reap_error, "reap failed")
        }
    }

    fn configured_result(kind: Option<io::ErrorKind>, message: &str) -> io::Result<()> {
        kind.map_or(Ok(()), |kind| Err(io::Error::new(kind, message)))
    }

    #[test]
    fn readiness_requires_success_byte() {
        assert!(wait_for_namespace_holder_ready(&mut &b"\x01"[..]).is_ok());

        let Err(unexpected) = wait_for_namespace_holder_ready(&mut &b"\x02"[..]) else {
            panic!("unexpected readiness byte must fail");
        };
        assert_eq!(unexpected.kind(), io::ErrorKind::InvalidData);

        let Err(missing) = wait_for_namespace_holder_ready(&mut &b""[..]) else {
            panic!("missing readiness byte must fail");
        };
        assert_eq!(missing.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn child_setns_failure_does_not_signal_readiness() {
        let mut ops = FakeOps {
            setns_fails: true,
            ..FakeOps::default()
        };

        assert_eq!(
            prepare_namespace_holder(&mut ops, NS_FD, READY_FD),
            Err(HolderStartupFailure::Setns)
        );
        assert_eq!(HolderStartupFailure::Setns.exit_code(), 1);
        assert_eq!(ops.events, vec![Event::Setns(NS_FD)]);
    }

    #[test]
    fn child_readiness_failure_occurs_after_setns() {
        let mut ops = FakeOps {
            readiness_fails: true,
            ..FakeOps::default()
        };

        assert_eq!(
            prepare_namespace_holder(&mut ops, NS_FD, READY_FD),
            Err(HolderStartupFailure::Readiness)
        );
        assert_eq!(HolderStartupFailure::Readiness.exit_code(), 2);
        assert_eq!(
            ops.events,
            vec![Event::Setns(NS_FD), Event::SignalReady(READY_FD)]
        );
    }

    #[test]
    fn child_success_signals_readiness_only_after_setns() {
        let mut ops = FakeOps::default();

        assert!(prepare_namespace_holder(&mut ops, NS_FD, READY_FD).is_ok());
        assert_eq!(
            ops.events,
            vec![Event::Setns(NS_FD), Event::SignalReady(READY_FD)]
        );
    }

    #[test]
    fn parent_readiness_failure_skips_move_then_terminates_and_reaps() {
        let mut ops = FakeOps::default();

        let Err(error) =
            complete_namespace_handoff(&mut ops, &mut &b""[..], "veth0", "/netns/web", PID)
        else {
            panic!("missing readiness must fail");
        };

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(error.to_string().contains("failed before handoff"));
        assert_eq!(ops.events, vec![Event::Terminate(PID), Event::Reap(PID)]);
    }

    #[test]
    fn parent_successful_handoff_terminates_and_reaps() {
        let mut ops = FakeOps::default();

        assert!(
            complete_namespace_handoff(&mut ops, &mut &b"\x01"[..], "veth0", "/netns/web", PID,)
                .is_ok()
        );
        assert_eq!(
            ops.events,
            vec![
                Event::MoveLink("veth0".to_string(), PID),
                Event::Terminate(PID),
                Event::Reap(PID),
            ]
        );
    }

    #[test]
    fn parent_move_failure_is_preserved_and_cleanup_still_runs() {
        let mut ops = FakeOps {
            move_error: Some(io::ErrorKind::PermissionDenied),
            ..FakeOps::default()
        };

        let Err(error) =
            complete_namespace_handoff(&mut ops, &mut &b"\x01"[..], "veth0", "/netns/web", PID)
        else {
            panic!("move failure must be returned");
        };

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "move failed");
        assert_eq!(
            ops.events,
            vec![
                Event::MoveLink("veth0".to_string(), PID),
                Event::Terminate(PID),
                Event::Reap(PID),
            ]
        );
    }

    #[test]
    fn cleanup_failures_are_observable_and_reap_follows_failed_terminate() {
        let mut ops = FakeOps {
            terminate_error: Some(io::ErrorKind::PermissionDenied),
            reap_error: Some(io::ErrorKind::InvalidInput),
            ..FakeOps::default()
        };

        let Err(error) =
            complete_namespace_handoff(&mut ops, &mut &b"\x01"[..], "veth0", "/netns/web", PID)
        else {
            panic!("cleanup failures must be returned");
        };

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("terminate failed"));
        assert!(error.to_string().contains("reap failed"));
        assert_eq!(
            ops.events,
            vec![
                Event::MoveLink("veth0".to_string(), PID),
                Event::Terminate(PID),
                Event::Reap(PID),
            ]
        );
    }

    #[test]
    fn cleanup_failure_augments_without_replacing_primary_failure() {
        let mut ops = FakeOps {
            move_error: Some(io::ErrorKind::NotFound),
            reap_error: Some(io::ErrorKind::InvalidInput),
            ..FakeOps::default()
        };

        let Err(error) =
            complete_namespace_handoff(&mut ops, &mut &b"\x01"[..], "veth0", "/netns/web", PID)
        else {
            panic!("primary move failure must be returned");
        };

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().starts_with("move failed;"));
        assert!(error.to_string().contains("reap failed"));
        assert_eq!(ops.events.last(), Some(&Event::Reap(PID)));
    }

    #[test]
    fn waitpid_retries_interrupted_calls() {
        let mut attempts = 0;

        assert!(
            retry_waitpid(PID, |pid| {
                attempts += 1;
                if attempts == 1 {
                    Err(io::Error::from(io::ErrorKind::Interrupted))
                } else {
                    Ok(pid)
                }
            })
            .is_ok()
        );
        assert_eq!(attempts, 2);
    }
}
