//! The Environment's own view of the name space, and nothing else's.
//!
//! This is the "split" half of split DNS, and the split is the whole point: the
//! resolver an Environment's Machines are pointed at answers the names that
//! Environment declared and refuses every other name outright. It is not a
//! forwarder and it is not a cache. A resolver that fell back to the host's
//! resolver would make an Environment-local name indistinguishable from a
//! public one, and would give a Machine a resolution path off its own fabric
//! that no declaration authorised.
//!
//! Nothing here does I/O. A query is bytes in and a response is bytes out, so
//! every rule below is decided on real wire-format messages in tests rather
//! than inferred from a running resolver.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

/// Fixed twelve-byte DNS header: id, flags, and the four section counts.
const HEADER_LEN: usize = 12;
/// The largest message this resolver reads or writes. 512 is the classic
/// UDP-without-EDNS limit; the Environment's answers are one A record each and
/// come nowhere near it, and a bound is what keeps a malformed length field
/// from being an allocation.
pub const MAX_MESSAGE: usize = 512;
/// A DNS name is at most 255 wire bytes and each label at most 63.
const MAX_NAME: usize = 255;
const MAX_LABEL: usize = 63;
/// The two high bits of a length octet mark a compression pointer.
const POINTER_MASK: u8 = 0xc0;

const TYPE_A: u16 = 1;
const CLASS_IN: u16 = 1;

/// How long a Machine may cache one of these answers.
///
/// Short, because an Environment's edge address is stable for the life of the
/// Environment but the Environment itself is not: a Machine that outlived a
/// `vz delete` and a re-`up` must not keep resolving a name to an address that
/// belonged to a fabric that no longer exists.
const TTL_SECONDS: u32 = 5;

/// Response codes this resolver produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rcode {
    NoError = 0,
    FormErr = 1,
    NxDomain = 3,
    NotImplemented = 4,
    Refused = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DnsError {
    #[error("message shorter than a DNS header")]
    Truncated,
    #[error("message is a response, not a query")]
    NotAQuery,
    #[error("query carries {0} questions; exactly one is answerable")]
    QuestionCount(u16),
    #[error("question name is malformed")]
    MalformedName,
}

/// One parsed question, with its name section kept verbatim.
///
/// The wire bytes are retained rather than re-encoded because the response
/// echoes the question section unchanged. Re-encoding a name that was accepted
/// on the way in is a second chance to disagree with the client about what it
/// asked, and a client compares the echoed question against its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    /// The name, lowercased and dot-separated, with no trailing dot.
    pub name: String,
    pub qtype: u16,
    pub qclass: u16,
    wire: Vec<u8>,
}

impl Question {
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A query this resolver was willing to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    id: u16,
    /// The client's recursion-desired bit, echoed so a resolver that never
    /// recurses still answers the question the client believes it asked.
    recursion_desired: bool,
    pub question: Question,
}

impl Query {
    pub fn question(&self) -> &Question {
        &self.question
    }
}

/// Read one query, or say why it is not one.
pub fn parse_query(bytes: &[u8]) -> Result<Query, DnsError> {
    if bytes.len() < HEADER_LEN || bytes.len() > MAX_MESSAGE {
        return Err(DnsError::Truncated);
    }
    let id = u16::from_be_bytes([bytes[0], bytes[1]]);
    let flags = u16::from_be_bytes([bytes[2], bytes[3]]);
    if flags & 0x8000 != 0 {
        return Err(DnsError::NotAQuery);
    }
    let questions = u16::from_be_bytes([bytes[4], bytes[5]]);
    if questions != 1 {
        return Err(DnsError::QuestionCount(questions));
    }
    let (name, consumed) = read_name(&bytes[HEADER_LEN..])?;
    let rest = HEADER_LEN + consumed;
    if bytes.len() < rest + 4 {
        return Err(DnsError::MalformedName);
    }
    let qtype = u16::from_be_bytes([bytes[rest], bytes[rest + 1]]);
    let qclass = u16::from_be_bytes([bytes[rest + 2], bytes[rest + 3]]);
    Ok(Query {
        id,
        recursion_desired: flags & 0x0100 != 0,
        question: Question {
            name,
            qtype,
            qclass,
            wire: bytes[HEADER_LEN..rest].to_vec(),
        },
    })
}

/// Read one uncompressed name from the start of `bytes`.
///
/// Compression pointers are refused rather than followed. A pointer in a
/// question section points backwards into a message this resolver has not
/// otherwise parsed, and following one is the classic way a name parser is made
/// to loop; a query that uses one is malformed for this resolver's purposes and
/// is told so.
fn read_name(bytes: &[u8]) -> Result<(String, usize), DnsError> {
    let mut name = String::new();
    let mut index = 0;
    loop {
        let length = *bytes.get(index).ok_or(DnsError::MalformedName)?;
        if length & POINTER_MASK != 0 {
            return Err(DnsError::MalformedName);
        }
        index += 1;
        if length == 0 {
            break;
        }
        let length = usize::from(length);
        if length > MAX_LABEL || index + length > bytes.len() || index + length > MAX_NAME {
            return Err(DnsError::MalformedName);
        }
        let label = &bytes[index..index + length];
        if !label
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-' || *byte == b'_')
        {
            return Err(DnsError::MalformedName);
        }
        if !name.is_empty() {
            name.push('.');
        }
        // Lowercased on the way in, so the table lookup is one comparison and a
        // client that asked in mixed case is answered the same as one that did
        // not. The echoed question keeps the client's own spelling.
        name.push_str(&String::from_utf8_lossy(label).to_ascii_lowercase());
        index += length;
    }
    Ok((name, index))
}

/// What this resolver decided about one query, before it is bytes again.
///
/// Separated from the encoding so a test can assert the decision itself. A test
/// that could only read the encoded response would be asserting the encoder as
/// much as the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The Environment declares this name; here is its address.
    Answer(Ipv4Addr),
    /// The Environment declares this name but not this record type.
    NoData,
    /// The Environment does not declare this name. It is not looked for
    /// anywhere else.
    Unknown,
    /// The query asked outside the class this resolver serves at all.
    OutOfScope,
}

/// Decide one query against the Environment's declared names.
///
/// `table` holds every name the Environment publishes, already lowercased. A
/// name absent from it is `Unknown` and stays unknown: there is no upstream.
pub fn decide(question: &Question, table: &BTreeMap<String, Ipv4Addr>) -> Decision {
    if question.qclass != CLASS_IN {
        return Decision::OutOfScope;
    }
    match table.get(question.name.as_str()) {
        Some(address) if question.qtype == TYPE_A => Decision::Answer(*address),
        Some(_) => Decision::NoData,
        None => Decision::Unknown,
    }
}

/// Encode the response one decision produces.
pub fn respond(query: &Query, decision: Decision) -> Vec<u8> {
    let (rcode, answers) = match decision {
        Decision::Answer(_) => (Rcode::NoError, 1_u16),
        Decision::NoData => (Rcode::NoError, 0),
        Decision::Unknown => (Rcode::NxDomain, 0),
        Decision::OutOfScope => (Rcode::Refused, 0),
    };
    // AA is set because this resolver is the only authority for these names:
    // there is no zone above the Environment to delegate from. RA is cleared
    // because it cannot recurse, so a client that asked for recursion is told
    // plainly that it did not get it rather than being left to assume the empty
    // answer means the name is absent from the whole world.
    let mut flags: u16 = 0x8000 | 0x0400 | rcode as u16;
    if query.recursion_desired {
        flags |= 0x0100;
    }
    let mut out = Vec::with_capacity(HEADER_LEN + query.question.wire.len() + 32);
    out.extend_from_slice(&query.id.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&1_u16.to_be_bytes());
    out.extend_from_slice(&answers.to_be_bytes());
    out.extend_from_slice(&0_u16.to_be_bytes());
    out.extend_from_slice(&0_u16.to_be_bytes());
    out.extend_from_slice(&query.question.wire);
    out.extend_from_slice(&query.question.qtype.to_be_bytes());
    out.extend_from_slice(&query.question.qclass.to_be_bytes());
    if let Decision::Answer(address) = decision {
        // The answer's name is a compression pointer to the question's, at the
        // fixed offset the question section always starts at in a message this
        // resolver wrote.
        out.extend_from_slice(&[POINTER_MASK, HEADER_LEN as u8]);
        out.extend_from_slice(&TYPE_A.to_be_bytes());
        out.extend_from_slice(&CLASS_IN.to_be_bytes());
        out.extend_from_slice(&TTL_SECONDS.to_be_bytes());
        out.extend_from_slice(&4_u16.to_be_bytes());
        out.extend_from_slice(&address.octets());
    }
    out
}

/// The response to a message this resolver could not read as a query.
///
/// A malformed message still gets an answer when it carried a readable id, so a
/// client fails immediately instead of waiting out its own timeout and
/// reporting "no resolver" for what was its own malformed question.
pub fn reject(bytes: &[u8], error: DnsError) -> Option<Vec<u8>> {
    if bytes.len() < HEADER_LEN || matches!(error, DnsError::NotAQuery) {
        return None;
    }
    let rcode = match error {
        DnsError::QuestionCount(_) => Rcode::NotImplemented,
        _ => Rcode::FormErr,
    };
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.extend_from_slice(&bytes[0..2]);
    out.extend_from_slice(&(0x8000_u16 | rcode as u16).to_be_bytes());
    out.extend_from_slice(&[0; 8]);
    Some(out)
}
