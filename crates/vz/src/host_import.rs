//! The guest-initiated authenticated relay that carries one host import.
//!
//! A host *export* is host-initiated: the host binds `127.0.0.1:<host_port>`
//! and dials the guest. A host *import* is the opposite direction, and the
//! product contract's rule for it is stricter:
//!
//! > Host imports require exact authenticated Environment/Machine grants to a
//! > declared host-loopback service, independently of external egress. NAT
//! > aliases and wildcard/LAN listeners are not authorization.
//!
//! Three properties follow, and this module's wire format exists to make each
//! of them structural rather than a convention the two ends happen to share:
//!
//! 1. **The guest never names a host destination.** The open frame carries a
//!    declaration *name* and nothing else addressable. The host terminator holds
//!    the only copy of the `127.0.0.1:<port>` pair and looks it up by that name.
//!    A guest that could send an address could send any address, which is
//!    exactly the "arbitrary host destination" the criterion denies.
//! 2. **The grant is authenticated, not positional.** Reaching the relay port is
//!    not authorization. Every declared import carries its own 32-byte
//!    credential, minted by the host for one Machine of one Environment for the
//!    lifetime of one boot and handed to that Machine's agent over its own
//!    private vsock control channel. Presenting the wrong name, no credential,
//!    or another import's credential is refused with no connection attempted.
//! 3. **The channel itself is Machine-scoped.** `Vm::vsock_listen` registers a
//!    listener on one VM's `VZVirtioSocketDevice`, so a frame arriving on it
//!    provably originated in that VM. The terminator's table therefore holds
//!    only the imports declared for that Machine: a sibling Machine, or a
//!    Machine in a sibling Environment, has a different listener with a
//!    different table and different credentials, and its name lookup misses.
//!
//! The frame is deliberately fixed-shape and tiny — magic, version, name
//! length, name, credential — so that a partial or malformed frame is a decode
//! error before any host socket is touched.
//!
//! ```text
//!   guest process ──▶ 127.0.0.1:<guest_port> (agent, loopback only)
//!                          │  vsock CID 2 : HOST_IMPORT_RELAY_PORT
//!                          ▼
//!                   open frame { name, credential }
//!                          │
//!                     host terminator ──▶ 127.0.0.1:<host_port>
//! ```

use std::fmt;

/// The vsock port the host terminator listens on, per Machine.
///
/// Distinct from [`crate::protocol::AGENT_PORT`] (7424) and from the native
/// bootstrap agent's 7420 because a relay stream is raw bytes after its open
/// frame and must never be multiplexed onto the agent's gRPC channel.
pub const HOST_IMPORT_RELAY_PORT: u32 = 7426;

/// Bytes in one import credential.
pub const CREDENTIAL_BYTES: usize = 32;

/// Longest import name the open frame can carry.
///
/// `name_len` is one byte, and the declaration name is a topology identifier
/// rather than free text, so 63 is generous. A longer declaration is refused at
/// admission rather than truncated here.
pub const MAX_NAME_BYTES: usize = 63;

/// Frame prefix. Present so that anything that is not this protocol — a stray
/// connection, a probe, a different version of the agent — fails to decode
/// instead of being interpreted as a name.
pub const OPEN_FRAME_MAGIC: [u8; 4] = *b"VZHI";

/// Wire revision of the open frame.
pub const OPEN_FRAME_VERSION: u8 = 1;

/// Fixed bytes before the variable-length name: magic, version, name length.
pub const OPEN_FRAME_HEADER_BYTES: usize = OPEN_FRAME_MAGIC.len() + 2;

/// The host accepted the grant and has connected the declared loopback service.
pub const REPLY_ACCEPTED: u8 = 0x01;

/// The host refused the grant. No host connection was attempted.
pub const REPLY_REFUSED: u8 = 0x00;

/// One declared import, as the host holds it for the life of one boot.
///
/// `host_port` never crosses to the guest: [`Self::guest_view`] is the only
/// projection the agent is given, and it deliberately cannot express a
/// destination.
#[derive(Clone, PartialEq, Eq)]
pub struct HostImportGrant {
    /// The declaration name, which is what the open frame carries.
    pub name: String,
    /// Loopback port the guest agent binds inside the Machine.
    pub guest_port: u16,
    /// Loopback port on the host this import terminates against. Host-only.
    pub host_port: u16,
    /// Per-import secret. Host-minted, one Machine, one boot.
    pub credential: [u8; CREDENTIAL_BYTES],
}

impl fmt::Debug for HostImportGrant {
    /// Never prints the credential. A grant reaches logs and evidence on every
    /// failure path, and a redacted field that is still comparable by length is
    /// all a diagnosis needs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostImportGrant")
            .field("name", &self.name)
            .field("guest_port", &self.guest_port)
            .field("host_port", &self.host_port)
            .field("credential", &"<redacted 32 bytes>")
            .finish()
    }
}

impl HostImportGrant {
    /// The half of this grant the guest agent is given.
    ///
    /// The host port is absent by construction rather than by omission, so a
    /// future field cannot leak it into the guest by being added to the wrong
    /// struct.
    pub fn guest_view(&self) -> GuestHostImportGrant {
        GuestHostImportGrant {
            name: self.name.clone(),
            guest_port: self.guest_port,
            credential: self.credential,
        }
    }
}

/// What the guest agent holds: a name, a loopback port to bind, and the secret
/// to present. Deliberately no host address of any kind.
#[derive(Clone, PartialEq, Eq)]
pub struct GuestHostImportGrant {
    pub name: String,
    pub guest_port: u16,
    pub credential: [u8; CREDENTIAL_BYTES],
}

impl fmt::Debug for GuestHostImportGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GuestHostImportGrant")
            .field("name", &self.name)
            .field("guest_port", &self.guest_port)
            .field("credential", &"<redacted 32 bytes>")
            .finish()
    }
}

/// The decoded open frame.
#[derive(Clone, PartialEq, Eq)]
pub struct HostImportOpen {
    pub name: String,
    pub credential: [u8; CREDENTIAL_BYTES],
}

impl fmt::Debug for HostImportOpen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostImportOpen")
            .field("name", &self.name)
            .field("credential", &"<redacted 32 bytes>")
            .finish()
    }
}

/// Why an open frame could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenFrameError {
    #[error("host import open frame does not start with the protocol magic")]
    Magic,
    #[error("host import open frame declares unsupported wire version {version}")]
    Version { version: u8 },
    #[error("host import open frame declares an empty declaration name")]
    EmptyName,
    #[error(
        "host import open frame declares a {declared}-byte name; at most {MAX_NAME_BYTES} is carried"
    )]
    NameTooLong { declared: usize },
    #[error("host import open frame is {actual} bytes; {expected} were declared")]
    Truncated { expected: usize, actual: usize },
    #[error("host import open frame name is not UTF-8")]
    NameEncoding,
}

/// Total bytes of an open frame carrying a name of `name_len` bytes.
pub const fn open_frame_len(name_len: usize) -> usize {
    OPEN_FRAME_HEADER_BYTES + name_len + CREDENTIAL_BYTES
}

/// The declared name length of a header, after validating magic and version.
///
/// Split out from [`decode_open`] so a reader can take the header first and
/// then read exactly the remaining bytes, rather than guessing a buffer size.
pub fn parse_open_header(header: &[u8; OPEN_FRAME_HEADER_BYTES]) -> Result<usize, OpenFrameError> {
    if header[..OPEN_FRAME_MAGIC.len()] != OPEN_FRAME_MAGIC {
        return Err(OpenFrameError::Magic);
    }
    let version = header[OPEN_FRAME_MAGIC.len()];
    if version != OPEN_FRAME_VERSION {
        return Err(OpenFrameError::Version { version });
    }
    let declared = usize::from(header[OPEN_FRAME_MAGIC.len() + 1]);
    if declared == 0 {
        return Err(OpenFrameError::EmptyName);
    }
    if declared > MAX_NAME_BYTES {
        return Err(OpenFrameError::NameTooLong { declared });
    }
    Ok(declared)
}

/// Serialize an open frame. Fails only on a name this format cannot carry.
pub fn encode_open(open: &HostImportOpen) -> Result<Vec<u8>, OpenFrameError> {
    let name = open.name.as_bytes();
    if name.is_empty() {
        return Err(OpenFrameError::EmptyName);
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(OpenFrameError::NameTooLong {
            declared: name.len(),
        });
    }
    let mut frame = Vec::with_capacity(open_frame_len(name.len()));
    frame.extend_from_slice(&OPEN_FRAME_MAGIC);
    frame.push(OPEN_FRAME_VERSION);
    // `name.len() <= MAX_NAME_BYTES` (63) was just proved, so this cast cannot
    // truncate.
    frame.push(name.len() as u8);
    frame.extend_from_slice(name);
    frame.extend_from_slice(&open.credential);
    Ok(frame)
}

/// Decode a complete open frame.
///
/// A frame with trailing bytes is refused rather than accepted-and-ignored: the
/// bytes after the frame are relay payload, and a decoder that silently
/// swallowed them would drop the first bytes the guest sent.
pub fn decode_open(frame: &[u8]) -> Result<HostImportOpen, OpenFrameError> {
    if frame.len() < OPEN_FRAME_HEADER_BYTES {
        return Err(OpenFrameError::Truncated {
            expected: OPEN_FRAME_HEADER_BYTES,
            actual: frame.len(),
        });
    }
    let mut header = [0u8; OPEN_FRAME_HEADER_BYTES];
    header.copy_from_slice(&frame[..OPEN_FRAME_HEADER_BYTES]);
    let name_len = parse_open_header(&header)?;
    let expected = open_frame_len(name_len);
    if frame.len() != expected {
        return Err(OpenFrameError::Truncated {
            expected,
            actual: frame.len(),
        });
    }
    let name_end = OPEN_FRAME_HEADER_BYTES + name_len;
    let name = std::str::from_utf8(&frame[OPEN_FRAME_HEADER_BYTES..name_end])
        .map_err(|_| OpenFrameError::NameEncoding)?
        .to_string();
    let mut credential = [0u8; CREDENTIAL_BYTES];
    credential.copy_from_slice(&frame[name_end..]);
    Ok(HostImportOpen { name, credential })
}

/// Compare two credentials without leaking their agreement through timing.
///
/// The arrays are fixed-length, so this is a plain fold over every byte with no
/// early exit. A short-circuiting `==` would let a caller that can retry learn
/// the credential one byte at a time.
#[must_use]
pub fn credentials_match(
    presented: &[u8; CREDENTIAL_BYTES],
    expected: &[u8; CREDENTIAL_BYTES],
) -> bool {
    let mut difference = 0u8;
    for index in 0..CREDENTIAL_BYTES {
        difference |= presented[index] ^ expected[index];
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn credential(byte: u8) -> [u8; CREDENTIAL_BYTES] {
        [byte; CREDENTIAL_BYTES]
    }

    #[test]
    fn an_open_frame_round_trips() {
        let open = HostImportOpen {
            name: "db".to_string(),
            credential: credential(7),
        };
        let frame = encode_open(&open).expect("encodable");
        assert_eq!(frame.len(), open_frame_len(2));
        assert_eq!(decode_open(&frame).expect("decodable"), open);
    }

    #[test]
    fn a_frame_that_is_not_this_protocol_is_refused() {
        let mut frame = encode_open(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1),
        })
        .expect("encodable");
        frame[0] = b'X';
        assert_eq!(decode_open(&frame), Err(OpenFrameError::Magic));
    }

    #[test]
    fn a_future_wire_version_is_refused_rather_than_guessed() {
        let mut frame = encode_open(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1),
        })
        .expect("encodable");
        frame[OPEN_FRAME_MAGIC.len()] = OPEN_FRAME_VERSION + 1;
        assert_eq!(
            decode_open(&frame),
            Err(OpenFrameError::Version {
                version: OPEN_FRAME_VERSION + 1
            })
        );
    }

    #[test]
    fn a_truncated_frame_is_refused_rather_than_zero_padded() {
        let frame = encode_open(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1),
        })
        .expect("encodable");
        for shorter in 0..frame.len() {
            assert!(
                matches!(
                    decode_open(&frame[..shorter]),
                    Err(OpenFrameError::Truncated { .. } | OpenFrameError::Magic)
                ),
                "{shorter} bytes must not decode"
            );
        }
    }

    #[test]
    fn trailing_bytes_are_refused_because_they_are_relay_payload() {
        let mut frame = encode_open(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1),
        })
        .expect("encodable");
        let expected = frame.len();
        frame.push(0);
        assert_eq!(
            decode_open(&frame),
            Err(OpenFrameError::Truncated {
                expected,
                actual: expected + 1
            })
        );
    }

    #[test]
    fn an_empty_or_oversized_name_cannot_be_encoded_or_decoded() {
        assert_eq!(
            encode_open(&HostImportOpen {
                name: String::new(),
                credential: credential(1),
            }),
            Err(OpenFrameError::EmptyName)
        );
        let long = "n".repeat(MAX_NAME_BYTES + 1);
        assert_eq!(
            encode_open(&HostImportOpen {
                name: long,
                credential: credential(1),
            }),
            Err(OpenFrameError::NameTooLong {
                declared: MAX_NAME_BYTES + 1
            })
        );
        let mut header = [0u8; OPEN_FRAME_HEADER_BYTES];
        header[..OPEN_FRAME_MAGIC.len()].copy_from_slice(&OPEN_FRAME_MAGIC);
        header[OPEN_FRAME_MAGIC.len()] = OPEN_FRAME_VERSION;
        header[OPEN_FRAME_MAGIC.len() + 1] = 0;
        assert_eq!(parse_open_header(&header), Err(OpenFrameError::EmptyName));
    }

    #[test]
    fn a_name_that_is_not_utf8_is_refused() {
        let mut frame = encode_open(&HostImportOpen {
            name: "db".to_string(),
            credential: credential(1),
        })
        .expect("encodable");
        frame[OPEN_FRAME_HEADER_BYTES] = 0xff;
        assert_eq!(decode_open(&frame), Err(OpenFrameError::NameEncoding));
    }

    #[test]
    fn credentials_match_only_themselves() {
        let expected = credential(9);
        assert!(credentials_match(&credential(9), &expected));
        assert!(!credentials_match(&credential(8), &expected));
        // A prefix match is not a match: the first byte agreeing must not be
        // reported as agreement.
        let mut near = expected;
        near[CREDENTIAL_BYTES - 1] ^= 1;
        assert!(!credentials_match(&near, &expected));
    }

    #[test]
    fn the_guest_view_of_a_grant_cannot_express_a_host_destination() {
        let grant = HostImportGrant {
            name: "db".to_string(),
            guest_port: 15432,
            // Deliberately not a substring of `guest_port`, so the assertion
            // below cannot pass on the guest port's digits.
            host_port: 5433,
            credential: credential(3),
        };
        let view = grant.guest_view();
        assert_eq!(view.name, "db");
        assert_eq!(view.guest_port, 15432);
        assert_eq!(view.credential, grant.credential);
        // The host port is not merely unset in the projection; the projection
        // has no field able to hold it. Debug output is the observable proof
        // that it also never reaches a log line.
        let rendered = format!("{view:?}");
        assert!(!rendered.contains("5433"), "{rendered}");
        assert!(!rendered.contains("host_port"), "{rendered}");
    }

    #[test]
    fn debug_output_never_carries_the_credential() {
        let grant = HostImportGrant {
            name: "db".to_string(),
            guest_port: 15432,
            host_port: 5432,
            credential: credential(0xab),
        };
        for rendered in [
            format!("{grant:?}"),
            format!("{:?}", grant.guest_view()),
            format!(
                "{:?}",
                HostImportOpen {
                    name: grant.name.clone(),
                    credential: grant.credential
                }
            ),
        ] {
            assert!(rendered.contains("redacted"), "{rendered}");
            assert!(!rendered.contains("171"), "{rendered}");
            assert!(!rendered.contains("ab, ab"), "{rendered}");
        }
    }

    #[test]
    fn the_relay_port_is_not_the_agent_port() {
        assert_ne!(HOST_IMPORT_RELAY_PORT, crate::protocol::AGENT_PORT);
    }
}
