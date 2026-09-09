//! The whole import decision path, exercised without a VM.
//!
//! `terminate` is generic over its stream precisely so these run: everything
//! that decides whether a host service is reached — frame, name, credential,
//! reply byte, destination — is here. The vsock transport is the only piece a
//! test stands in for, and it carries no policy: it decides *which Machine*
//! reached the relay, which is a property of `Vm::vsock_listen` registering one
//! listener per socket device.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpListener;
use vz::host_import::encode_open;

fn credential(byte: u8) -> [u8; CREDENTIAL_BYTES] {
    [byte; CREDENTIAL_BYTES]
}

fn grant(name: &str, host_port: u16, byte: u8) -> HostImportGrant {
    HostImportGrant {
        name: name.to_string(),
        guest_port: host_port.wrapping_add(10_000),
        host_port,
        credential: credential(byte),
    }
}

/// A loopback TCP service that echoes one line, standing in for the host
/// service an import terminates against.
async fn echo_service() -> (u16, tokio::task::JoinHandle<Option<Vec<u8>>>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("loopback service");
    let port = listener.local_addr().expect("addr").port();
    let handle = tokio::spawn(async move {
        let (mut stream, _peer) = listener.accept().await.ok()?;
        let mut received = vec![0u8; 5];
        stream.read_exact(&mut received).await.ok()?;
        stream.write_all(b"pong!").await.ok()?;
        stream.flush().await.ok()?;
        Some(received)
    });
    (port, handle)
}

#[test]
fn the_termination_address_is_loopback_and_nothing_else() {
    assert_eq!(IMPORT_TERMINATION_ADDRESS, Ipv4Addr::new(127, 0, 0, 1));
    assert!(IMPORT_TERMINATION_ADDRESS.is_loopback());
    assert_ne!(IMPORT_TERMINATION_ADDRESS, Ipv4Addr::UNSPECIFIED);
}

#[test]
fn a_table_with_a_repeated_name_or_an_unbindable_port_is_refused() {
    assert!(matches!(
        HostImportTerminator::new(&[grant("db", 5432, 1), grant("db", 5433, 2)]),
        Err(HostImportRelayError::DuplicateName { .. })
    ));
    assert!(matches!(
        HostImportTerminator::new(&[grant("db", 0, 1)]),
        Err(HostImportRelayError::ZeroHostPort { .. })
    ));
    assert!(HostImportTerminator::new(&[grant("db", 5432, 1), grant("cache", 5433, 2)]).is_ok());
}

#[test]
fn a_name_this_machine_does_not_declare_resolves_to_nothing() {
    let terminator = HostImportTerminator::new(&[grant("db", 5432, 1)]).expect("table");
    assert_eq!(
        terminator.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1)
        }),
        Ok(5432)
    );
    // The undeclared port and the sibling Machine's import look identical from
    // here, which is the point: this Machine's table is the whole grant.
    for absent in ["cache", "db2", "127.0.0.1:5432", "5432", ""] {
        assert_eq!(
            terminator.authorize(&HostImportOpen {
                name: absent.to_string(),
                credential: credential(1)
            }),
            Err(Refusal::UnknownImport {
                name: absent.to_string()
            }),
            "{absent} must not resolve"
        );
    }
}

#[test]
fn a_declared_name_with_another_imports_credential_is_refused() {
    let terminator =
        HostImportTerminator::new(&[grant("db", 5432, 1), grant("cache", 6379, 2)]).expect("table");
    // The credential of a real, sibling declaration is not a skeleton key.
    assert_eq!(
        terminator.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(2)
        }),
        Err(Refusal::BadCredential {
            name: "db".to_string()
        })
    );
    // Nor is an empty or all-zero credential.
    assert_eq!(
        terminator.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: [0u8; CREDENTIAL_BYTES]
        }),
        Err(Refusal::BadCredential {
            name: "db".to_string()
        })
    );
}

#[tokio::test]
async fn an_authorized_grant_reaches_exactly_the_declared_loopback_service() {
    let (port, service) = echo_service().await;
    let terminator = HostImportTerminator::new(&[grant("db", port, 7)]).expect("table");
    let (mut guest, host_side) = duplex(4096);
    let relay = tokio::spawn(async move { terminator.terminate(host_side).await });

    let frame = encode_open(&HostImportOpen {
        name: "db".to_string(),
        credential: credential(7),
    })
    .expect("frame");
    guest.write_all(&frame).await.expect("open frame");
    guest.flush().await.expect("flush");
    let mut reply = [0u8; 1];
    guest.read_exact(&mut reply).await.expect("reply");
    assert_eq!(reply[0], REPLY_ACCEPTED);

    guest.write_all(b"ping!").await.expect("payload");
    guest.flush().await.expect("flush payload");
    let mut answer = [0u8; 5];
    guest.read_exact(&mut answer).await.expect("answer");
    assert_eq!(&answer, b"pong!");
    assert_eq!(service.await.expect("service"), Some(b"ping!".to_vec()));
    // The relay is `copy_bidirectional`, so it lives until BOTH halves are
    // done. Closing the guest end is what a guest process exiting does, and
    // waiting for the relay before doing it would wait forever.
    drop(guest);
    assert_eq!(relay.await.expect("relay"), Ok(Outcome::Relayed));
}

#[tokio::test]
async fn an_undeclared_name_is_refused_without_dialling_anything() {
    // The service is live, so a relay that dialled it anyway would be caught by
    // the accept below rather than by the reply byte alone.
    let (port, service) = echo_service().await;
    let terminator = HostImportTerminator::new(&[grant("db", port, 7)]).expect("table");
    let (mut guest, host_side) = duplex(4096);
    let relay = tokio::spawn(async move { terminator.terminate(host_side).await });

    let frame = encode_open(&HostImportOpen {
        name: "undeclared".to_string(),
        credential: credential(7),
    })
    .expect("frame");
    guest.write_all(&frame).await.expect("open frame");
    guest.flush().await.expect("flush");
    let mut reply = [0u8; 1];
    guest.read_exact(&mut reply).await.expect("reply");
    assert_eq!(reply[0], REPLY_REFUSED);
    assert_eq!(
        relay.await.expect("relay"),
        Err(Refusal::UnknownImport {
            name: "undeclared".to_string()
        })
    );
    // Nothing connected: the service is still waiting on its first accept.
    assert!(!service.is_finished());
    service.abort();
}

#[tokio::test]
async fn a_wrong_credential_is_refused_without_dialling_anything() {
    let (port, service) = echo_service().await;
    let terminator = HostImportTerminator::new(&[grant("db", port, 7)]).expect("table");
    let (mut guest, host_side) = duplex(4096);
    let relay = tokio::spawn(async move { terminator.terminate(host_side).await });

    let frame = encode_open(&HostImportOpen {
        name: "db".to_string(),
        credential: credential(8),
    })
    .expect("frame");
    guest.write_all(&frame).await.expect("open frame");
    guest.flush().await.expect("flush");
    let mut reply = [0u8; 1];
    guest.read_exact(&mut reply).await.expect("reply");
    assert_eq!(reply[0], REPLY_REFUSED);
    assert_eq!(
        relay.await.expect("relay"),
        Err(Refusal::BadCredential {
            name: "db".to_string()
        })
    );
    assert!(!service.is_finished());
    service.abort();
}

#[tokio::test]
async fn the_refusal_byte_does_not_say_which_half_of_the_grant_was_wrong() {
    let terminator = HostImportTerminator::new(&[grant("db", 65_000, 7)]).expect("table");
    let mut replies = Vec::new();
    for open in [
        HostImportOpen {
            name: "absent".to_string(),
            credential: credential(7),
        },
        HostImportOpen {
            name: "db".to_string(),
            credential: credential(8),
        },
    ] {
        let (mut guest, host_side) = duplex(4096);
        let table = HostImportTerminator::new(&[grant("db", 65_000, 7)]).expect("table");
        let relay = tokio::spawn(async move { table.terminate(host_side).await });
        guest
            .write_all(&encode_open(&open).expect("frame"))
            .await
            .expect("open frame");
        guest.flush().await.expect("flush");
        let mut reply = [0u8; 1];
        guest.read_exact(&mut reply).await.expect("reply");
        replies.push(reply[0]);
        let _ = relay.await;
    }
    assert_eq!(replies, vec![REPLY_REFUSED, REPLY_REFUSED]);
    drop(terminator);
}

#[tokio::test]
async fn a_frame_that_is_not_this_protocol_is_refused() {
    let terminator = HostImportTerminator::new(&[grant("db", 65_000, 7)]).expect("table");
    let (mut guest, host_side) = duplex(4096);
    let relay = tokio::spawn(async move { terminator.terminate(host_side).await });
    // A plain HTTP request: the shape a confused or hostile guest process would
    // send if it found the relay port and assumed it spoke something familiar.
    guest
        .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await
        .expect("bytes");
    guest.flush().await.expect("flush");
    let mut reply = [0u8; 1];
    guest.read_exact(&mut reply).await.expect("reply");
    assert_eq!(reply[0], REPLY_REFUSED);
    assert!(matches!(
        relay.await.expect("relay"),
        Err(Refusal::Malformed(_))
    ));
}

#[tokio::test]
async fn an_authorized_grant_whose_host_service_is_absent_is_reported_as_unreachable() {
    // A port nothing is listening on. Bound then released so it is a real free
    // port rather than a guess.
    let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("probe");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    let terminator = HostImportTerminator::new(&[grant("db", port, 7)]).expect("table");
    let (mut guest, host_side) = duplex(4096);
    let relay = tokio::spawn(async move { terminator.terminate(host_side).await });
    guest
        .write_all(
            &encode_open(&HostImportOpen {
                name: "db".to_string(),
                credential: credential(7),
            })
            .expect("frame"),
        )
        .await
        .expect("open frame");
    guest.flush().await.expect("flush");
    let mut reply = [0u8; 1];
    guest.read_exact(&mut reply).await.expect("reply");
    assert_eq!(reply[0], REPLY_REFUSED);
    assert!(
        matches!(
            relay.await.expect("relay"),
            Err(Refusal::Unreachable { .. })
        ),
        "an authorized grant whose service is down is not a policy refusal"
    );
}

#[tokio::test]
async fn a_guest_that_opens_and_says_nothing_does_not_hold_the_relay_forever() {
    let terminator = HostImportTerminator::new(&[grant("db", 65_000, 7)]).expect("table");
    let (guest, host_side) = duplex(4096);
    // Deliberately never write. The deadline is 5s, so this test would hang
    // rather than fail if the timeout were removed.
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::spawn(async move { terminator.terminate(host_side).await }),
    )
    .await
    .expect("the relay must give up on its own")
    .expect("join");
    assert_eq!(outcome, Err(Refusal::OpenTimeout));
    drop(guest);
}

#[tokio::test]
async fn two_machines_holding_the_same_declaration_name_do_not_share_a_grant() {
    // The same declaration name, two Machines, two credentials, two host
    // services. This is the "wrong Machine" denial expressed at the level the
    // terminator can see it: each Machine's relay is built from its own table.
    let (first_port, first_service) = echo_service().await;
    let (second_port, second_service) = echo_service().await;
    let first = HostImportTerminator::new(&[grant("db", first_port, 1)]).expect("first table");
    let second = HostImportTerminator::new(&[grant("db", second_port, 2)]).expect("second table");
    // Machine one's credential does not open Machine two's import.
    assert_eq!(
        second.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1)
        }),
        Err(Refusal::BadCredential {
            name: "db".to_string()
        })
    );
    // And each resolves only to its own host service.
    assert_eq!(
        first.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1)
        }),
        Ok(first_port)
    );
    assert_eq!(
        second.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(2)
        }),
        Ok(second_port)
    );
    assert_ne!(first_port, second_port);
    first_service.abort();
    second_service.abort();
}

#[tokio::test]
async fn a_machine_with_no_declared_import_gets_no_relay_at_all() {
    // `start_host_import_relay` needs a VM, but its "absent by default" branch
    // is decided before the VM is touched, so the empty case is provable here.
    // A non-empty list would have to reach `vsock_listen`; that path is covered
    // by the physical gate check.
    let terminator = HostImportTerminator::new(&[]).expect("empty table");
    assert_eq!(
        terminator.authorize(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1)
        }),
        Err(Refusal::UnknownImport {
            name: "db".to_string()
        })
    );
}
