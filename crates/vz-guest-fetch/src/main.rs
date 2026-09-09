//! A certificate-verifying HTTPS client for the Developer Linux guest image.
//!
//! Criterion 6 asks that a client inside a Machine reach a declared API through
//! the Environment's own edge: through its split DNS, through TLS it can
//! actually verify, through routed ingress and through the edge's address
//! translation. Every one of those clauses needs a client, and the image had
//! none. BusyBox 1.37.0's `ssl_client` reads one handshake message per TLS
//! record while rustls coalesces its TLS 1.2 server flight into a single
//! record, so the handshake deadlocks against this edge and no server setting
//! fixes it; and `networking/wget.c` force-sets no-check-certificate and says
//! so, meaning a handshake that did complete would verify nothing. A client
//! that cannot tell this Environment's authority from any other proves none of
//! the TLS clause.
//!
//! So this is deliberately small and deliberately rigid:
//!
//! * one HTTPS GET, and no other scheme -- `http://` is refused rather than
//!   silently downgraded, because a check that accidentally fetched plaintext
//!   would report a TLS clause it never exercised;
//! * the chain is verified, always. There is no `--insecure`, no
//!   `--no-check-certificate` and no environment variable that turns
//!   verification off. An off switch would let a future check pass without
//!   proving anything about TLS, which is exactly the failure this binary
//!   exists to remove;
//! * the trust anchors come from a file on disk, defaulting to the Mozilla
//!   bundle this product already pins into the image at
//!   `/etc/vz/ca-certificates.crt`. They are not compiled in. The bundle the
//!   product ships is therefore the bundle the guest actually trusts, and an
//!   Environment's own authority is trusted only when it is named -- so
//!   fetching an Environment edge against the default bundle *fails*, which is
//!   the negative half of the proof;
//! * the response body goes to stdout byte for byte and nothing else does. The
//!   receipt and every failure envelope go to stderr as one JSON line, so a
//!   caller can never mistake a diagnosis for a body.
//!
//! Exit status is 0 on a completed, verified exchange and non-zero otherwise,
//! with the reason named in the envelope. The reasons are distinct on purpose:
//! a negative test that passed because DNS failed, rather than because the
//! certificate was refused, would be vacuous.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use rustls::RootCertStore;
use rustls_pki_types::ServerName;

const SCHEMA_VERSION: u32 = 1;

/// The bundle `linux/ca-trust` installs into every Developer image. Naming it
/// as the default is what gives that bundle a consumer: before this binary
/// existed nothing in the guest read it.
const DEFAULT_TRUST_BUNDLE: &str = "/etc/vz/ca-certificates.crt";

const DEFAULT_TIMEOUT_MILLIS: u64 = 10_000;
/// A bound on what one GET may return, so a hostile or looping origin cannot
/// turn a fetch into an allocation.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Every option this binary accepts. Written down once so the test that
/// asserts no verification escape hatch exists is asserting about the list
/// `main` actually parses with.
const ALLOWED: &[&str] = &["url", "ca-file", "timeout-millis"];

const USAGE: &str = "usage:\n  \
    vz-guest-fetch get --url https://<host>[:<port>]/<path> [--ca-file <path>] [--timeout-millis <n>]\n\n\
    The chain is always verified. There is no option to disable verification.";

/// Why a fetch did not produce a verified response.
///
/// These are separated so a caller can assert on the *kind* of refusal.
/// "the fetch failed" is compatible with the name never resolving, and a
/// negative TLS test that accepted that would prove nothing about TLS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    InvalidArguments,
    TrustBundleUnreadable,
    /// The file was readable and held no usable trust anchor. Trusting nothing
    /// is not the same as trusting everything, and it is not silently allowed.
    TrustBundleEmpty,
    ResolveFailed,
    ConnectFailed,
    /// The peer's chain was refused: unknown issuer, wrong name, expired.
    /// This is the reason a "must be refused" assertion looks for.
    CertificateRejected,
    TlsFailed,
    HttpFailed,
    OutputFailed,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::InvalidArguments => "invalid_arguments",
            Reason::TrustBundleUnreadable => "trust_bundle_unreadable",
            Reason::TrustBundleEmpty => "trust_bundle_empty",
            Reason::ResolveFailed => "resolve_failed",
            Reason::ConnectFailed => "connect_failed",
            Reason::CertificateRejected => "certificate_rejected",
            Reason::TlsFailed => "tls_failed",
            Reason::HttpFailed => "http_failed",
            Reason::OutputFailed => "output_failed",
        }
    }

    /// A distinct status per reason, so a caller with no way to read stderr
    /// still learns more than "it failed".
    fn exit_code(self) -> u8 {
        match self {
            Reason::InvalidArguments => 2,
            Reason::TrustBundleUnreadable => 3,
            Reason::TrustBundleEmpty => 4,
            Reason::ResolveFailed => 5,
            Reason::ConnectFailed => 6,
            Reason::CertificateRejected => 7,
            Reason::TlsFailed => 8,
            Reason::HttpFailed => 9,
            Reason::OutputFailed => 10,
        }
    }
}

#[derive(Debug)]
struct Failure {
    reason: Reason,
    detail: String,
}

impl Failure {
    fn new(reason: Reason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

/// Diagnoses go to stderr, never to stdout: stdout carries the response body
/// byte for byte, and a caller that had to strip an envelope out of it could
/// not tell a body that happened to look like one from the real thing.
fn emit_error(failure: &Failure) -> ExitCode {
    let document = serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "vz-guest-fetch-error",
        "reason": failure.reason.as_str(),
        "detail": failure.detail,
    });
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{document}");
    let _ = err.flush();
    ExitCode::from(failure.reason.exit_code())
}

fn emit_receipt(receipt: &serde_json::Value) {
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{receipt}");
    let _ = err.flush();
}

// -- arguments ---------------------------------------------------------------

#[derive(Debug)]
struct Options {
    values: std::collections::BTreeMap<String, String>,
}

impl Options {
    /// Unknown and valueless options are refused rather than ignored. A client
    /// that quietly dropped `--ca-file` would fall back to the default bundle
    /// and report a refusal that was entirely its own doing; one that quietly
    /// accepted `--insecure` would be worse.
    fn parse(argv: &[String], allowed: &[&str]) -> Result<Self, String> {
        let mut values = std::collections::BTreeMap::new();
        let mut index = 0;
        while index < argv.len() {
            let name = argv[index]
                .strip_prefix("--")
                .ok_or_else(|| format!("expected an option, found {:?}", argv[index]))?;
            if !allowed.contains(&name) {
                return Err(format!("unknown option --{name}"));
            }
            let value = argv
                .get(index + 1)
                .ok_or_else(|| format!("--{name} requires a value"))?;
            if value.starts_with("--") {
                return Err(format!("--{name} requires a value"));
            }
            if values.insert(name.to_string(), value.clone()).is_some() {
                return Err(format!("--{name} given more than once"));
            }
            index += 2;
        }
        Ok(Self { values })
    }

    fn required(&self, name: &str) -> Result<&str, String> {
        self.values
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| format!("missing required option --{name}"))
    }

    fn optional(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }
}

// -- the target --------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
    /// Path and query exactly as the request line will carry them.
    path: String,
}

impl Target {
    /// The `Host:` header. A non-default port belongs in it, because an origin
    /// behind the edge may serve by name and would otherwise be told a name it
    /// does not recognise.
    fn authority(&self) -> String {
        if self.port == 443 {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// Parse `https://host[:port][/path]` and refuse everything else.
///
/// `http://` is refused rather than upgraded or downgraded: this binary exists
/// to exercise a TLS clause, and a fetch that silently spoke plaintext would
/// report success for a path that never handshook.
fn parse_url(url: &str) -> Result<Target, String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("only https:// URLs are fetched, found {url:?}"))?;
    if rest.is_empty() {
        return Err("https:// URL carries no host".to_string());
    }
    let (authority, path) = match rest.find('/') {
        Some(cut) => (&rest[..cut], rest[cut..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.contains('@') {
        return Err("credentials in a URL are not accepted".to_string());
    }
    if path.contains('#') {
        return Err("a fragment is not part of a request line".to_string());
    }
    let (host, port) = match authority.rsplit_once(':') {
        // A bare IPv6 literal has colons of its own; only a bracketed one is
        // unambiguous, and this client has no need of either.
        Some((head, tail)) if !head.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) => {
            let port: u16 = tail
                .parse()
                .map_err(|_| format!("port {tail:?} is not a TCP port"))?;
            if port == 0 {
                return Err("port 0 is not a TCP port".to_string());
            }
            (head.to_string(), port)
        }
        _ => (authority.to_string(), 443),
    };
    if host.is_empty() || host.contains(':') || host.contains('[') {
        return Err(format!("{host:?} is not a host this client fetches"));
    }
    Ok(Target {
        host: host.to_ascii_lowercase(),
        port,
        path,
    })
}

// -- trust -------------------------------------------------------------------

/// Load trust anchors from one PEM file, and from nowhere else.
///
/// No platform store, no compiled-in root set. The file is the whole of what
/// this client trusts, so what a caller passed (or did not pass) is exactly
/// what the verification below means.
fn load_trust(path: &str) -> Result<(RootCertStore, usize), Failure> {
    let pem = std::fs::read(path).map_err(|error| {
        Failure::new(
            Reason::TrustBundleUnreadable,
            format!("{path}: {error}; {}", trust_hint(path)),
        )
    })?;
    let mut cursor = std::io::Cursor::new(pem);
    let mut roots = RootCertStore::empty();
    let mut added = 0usize;
    for certificate in rustls_pemfile::certs(&mut cursor) {
        let certificate = certificate.map_err(|error| {
            Failure::new(
                Reason::TrustBundleUnreadable,
                format!("{path}: malformed PEM: {error}"),
            )
        })?;
        roots.add(certificate).map_err(|error| {
            Failure::new(
                Reason::TrustBundleUnreadable,
                format!("{path}: rejected trust anchor: {error}"),
            )
        })?;
        added += 1;
    }
    if added == 0 {
        return Err(Failure::new(
            Reason::TrustBundleEmpty,
            format!("{path} holds no CERTIFICATE block; trusting nothing is refused outright"),
        ));
    }
    Ok((roots, added))
}

fn trust_hint(path: &str) -> String {
    if path == DEFAULT_TRUST_BUNDLE {
        format!(
            "{DEFAULT_TRUST_BUNDLE} is the image's pinned bundle; pass --ca-file to name another"
        )
    } else {
        "pass --ca-file with a readable PEM bundle".to_string()
    }
}

// -- the fetch ---------------------------------------------------------------

#[derive(Debug)]
struct Response {
    status: u16,
    peer: SocketAddr,
    protocol: String,
    body: Vec<u8>,
}

/// One verified HTTPS GET.
///
/// The connection is opened by name, so the Environment's resolver is what
/// decides where this goes; the address it landed on is reported back, which is
/// how a caller learns the name led to the edge rather than to the origin
/// behind it.
fn fetch(target: &Target, roots: RootCertStore, timeout: Duration) -> Result<Response, Failure> {
    // Named rather than taken from process-global state, for the same reason
    // the edge names its own: which provider decides what this client will
    // negotiate is not a setting some other part of a process gets to install.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| Failure::new(Reason::TlsFailed, error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(target.host.clone())
        .map_err(|error| Failure::new(Reason::InvalidArguments, format!("{error}")))?;
    let mut connection = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|error| Failure::new(Reason::TlsFailed, error.to_string()))?;

    let addresses: Vec<SocketAddr> = (target.host.as_str(), target.port)
        .to_socket_addrs()
        .map_err(|error| {
            Failure::new(
                Reason::ResolveFailed,
                format!("{}:{}: {error}", target.host, target.port),
            )
        })?
        .collect();
    let first = addresses.first().copied().ok_or_else(|| {
        Failure::new(
            Reason::ResolveFailed,
            format!("{}:{} resolved to no address", target.host, target.port),
        )
    })?;
    let mut socket = TcpStream::connect_timeout(&first, timeout)
        .map_err(|error| Failure::new(Reason::ConnectFailed, format!("{first}: {error}")))?;
    let peer = socket
        .peer_addr()
        .map_err(|error| Failure::new(Reason::ConnectFailed, error.to_string()))?;
    // Deadlines on both directions: a fetch inside a check must fail loudly
    // rather than hang until the lane's own timeout kills it, because a killed
    // invocation reports no reason at all.
    socket
        .set_read_timeout(Some(timeout))
        .and_then(|()| socket.set_write_timeout(Some(timeout)))
        .map_err(|error| Failure::new(Reason::ConnectFailed, error.to_string()))?;

    // The handshake is completed on its own, before a single request byte is
    // written, so a refusal here is unambiguously a refusal of the peer's
    // chain rather than something that went wrong mid-exchange.
    connection
        .complete_io(&mut socket)
        .map_err(|error| classify_tls(&error))?;
    let protocol = connection
        .protocol_version()
        .map(|version| format!("{version:?}"))
        .unwrap_or_else(|| "unknown".to_string());

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: vz-guest-fetch/{}\r\nAccept: */*\r\nConnection: close\r\n\r\n",
        target.path,
        target.authority(),
        env!("CARGO_PKG_VERSION"),
    );
    let mut stream = rustls::Stream::new(&mut connection, &mut socket);
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(|error| classify_tls(&error))?;
    let raw = read_response(&mut stream)?;
    let (status, body) = parse_response(&raw)?;
    Ok(Response {
        status,
        peer,
        protocol,
        body,
    })
}

/// Tell a rejected certificate apart from every other transport failure.
///
/// rustls surfaces its own errors through `io::Error`, so the distinction a
/// caller needs -- "the peer was refused" versus "the peer went away" -- is
/// only available by looking inside.
fn classify_tls(error: &std::io::Error) -> Failure {
    if let Some(inner) = error
        .get_ref()
        .and_then(|e| e.downcast_ref::<rustls::Error>())
        && let rustls::Error::InvalidCertificate(reason) = inner
    {
        return Failure::new(
            Reason::CertificateRejected,
            format!("invalid peer certificate: {reason:?}"),
        );
    }
    Failure::new(Reason::TlsFailed, error.to_string())
}

/// Read until the origin is done, bounded.
///
/// The request said `Connection: close`, so end-of-stream is the framing.
/// `Content-Length` and `Transfer-Encoding: chunked` are still honoured when
/// present, so an origin that keeps the connection open anyway is not waited
/// out until the deadline.
fn read_response(stream: &mut impl Read) -> Result<Vec<u8>, Failure> {
    let mut raw: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    loop {
        if complete(&raw) {
            break;
        }
        let read = match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => read,
            // A close_notify-less shutdown is how plenty of origins end a
            // `Connection: close` response; the bytes already read stand.
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => {
                let failure = classify_tls(&error);
                if raw.is_empty() {
                    return Err(failure);
                }
                return Err(Failure::new(
                    Reason::HttpFailed,
                    format!("{} after {} bytes", failure.detail, raw.len()),
                ));
            }
        };
        if raw.len() + read > MAX_RESPONSE_BYTES {
            return Err(Failure::new(
                Reason::HttpFailed,
                format!("response exceeds {MAX_RESPONSE_BYTES} bytes"),
            ));
        }
        raw.extend_from_slice(&chunk[..read]);
    }
    Ok(raw)
}

/// Whether the bytes so far are a whole response by their own framing.
fn complete(raw: &[u8]) -> bool {
    let Some(head) = header_end(raw) else {
        return false;
    };
    let headers = String::from_utf8_lossy(&raw[..head]);
    let body = &raw[head..];
    if header_value(&headers, "transfer-encoding")
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
    {
        return decode_chunked(body).is_ok();
    }
    match header_value(&headers, "content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        Some(length) => body.len() >= length,
        None => false,
    }
}

fn header_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|at| at + 4)
}

fn header_value(headers: &str, name: &str) -> Option<String> {
    headers
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim().to_string())
}

/// Split a raw response into its status and its decoded body.
fn parse_response(raw: &[u8]) -> Result<(u16, Vec<u8>), Failure> {
    let head = header_end(raw).ok_or_else(|| {
        Failure::new(
            Reason::HttpFailed,
            format!("no header terminator in {} bytes of response", raw.len()),
        )
    })?;
    if head > MAX_HEADER_BYTES {
        return Err(Failure::new(
            Reason::HttpFailed,
            format!("response header exceeds {MAX_HEADER_BYTES} bytes"),
        ));
    }
    let headers = String::from_utf8_lossy(&raw[..head]).to_string();
    let status_line = headers.split("\r\n").next().unwrap_or_default();
    let mut fields = status_line.split(' ');
    let version = fields.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(Failure::new(
            Reason::HttpFailed,
            format!("not an HTTP/1.x response: {status_line:?}"),
        ));
    }
    let status: u16 = fields
        .next()
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| {
            Failure::new(
                Reason::HttpFailed,
                format!("no status code in {status_line:?}"),
            )
        })?;
    let body = &raw[head..];
    if header_value(&headers, "transfer-encoding")
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
    {
        let decoded =
            decode_chunked(body).map_err(|detail| Failure::new(Reason::HttpFailed, detail))?;
        return Ok((status, decoded));
    }
    if let Some(length) = header_value(&headers, "content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        if body.len() < length {
            return Err(Failure::new(
                Reason::HttpFailed,
                format!(
                    "body is {} bytes, Content-Length declared {length}",
                    body.len()
                ),
            ));
        }
        return Ok((status, body[..length].to_vec()));
    }
    Ok((status, body.to_vec()))
}

/// Decode a chunked body, or say it is not yet whole.
///
/// `Err` means "incomplete or malformed", which is also what `complete` needs
/// in order to decide whether to keep reading.
fn decode_chunked(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| "chunked body ends mid-size".to_string())?;
        let size_field = String::from_utf8_lossy(&rest[..line_end]);
        // A chunk extension follows a semicolon and is not part of the size.
        let size_text = size_field.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| format!("chunk size {size_text:?} is not hexadecimal"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if rest.len() < size + 2 {
            return Err("chunked body ends mid-chunk".to_string());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

// -- entry point -------------------------------------------------------------

fn run(options: &Options) -> Result<serde_json::Value, Failure> {
    let url = options
        .required("url")
        .map_err(|detail| Failure::new(Reason::InvalidArguments, detail))?;
    let target = parse_url(url).map_err(|detail| Failure::new(Reason::InvalidArguments, detail))?;
    let ca_file = options.optional("ca-file").unwrap_or(DEFAULT_TRUST_BUNDLE);
    let timeout_millis: u64 = match options.optional("timeout-millis") {
        Some(text) => text.parse().map_err(|_| {
            Failure::new(
                Reason::InvalidArguments,
                format!("--timeout-millis {text:?} is not a number of milliseconds"),
            )
        })?,
        None => DEFAULT_TIMEOUT_MILLIS,
    };
    if timeout_millis == 0 {
        return Err(Failure::new(
            Reason::InvalidArguments,
            "--timeout-millis 0 would never attempt anything",
        ));
    }
    let (roots, anchors) = load_trust(ca_file)?;
    let response = fetch(&target, roots, Duration::from_millis(timeout_millis))?;
    let mut out = std::io::stdout().lock();
    out.write_all(&response.body)
        .and_then(|()| out.flush())
        .map_err(|error| Failure::new(Reason::OutputFailed, error.to_string()))?;
    Ok(serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "vz-guest-fetch-response",
        "url": url,
        "host": target.host,
        "port": target.port,
        // The address the name led to. This is what makes "the client reached
        // the edge, not the origin" a fact the client itself reports.
        "peer": response.peer.ip().to_string(),
        "peer_port": response.peer.port(),
        "status": response.status,
        "protocol": response.protocol,
        "body_bytes": response.body.len(),
        "trust_bundle": ca_file,
        "trust_anchors": anchors,
        "verified": true,
    }))
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(mode) = argv.first().cloned() else {
        return emit_error(&Failure::new(Reason::InvalidArguments, USAGE));
    };
    if mode != "get" {
        return emit_error(&Failure::new(
            Reason::InvalidArguments,
            format!("unknown mode {mode:?}; {USAGE}"),
        ));
    }
    let options = match Options::parse(&argv[1..], ALLOWED) {
        Ok(options) => options,
        Err(detail) => {
            return emit_error(&Failure::new(
                Reason::InvalidArguments,
                format!("{detail}; {USAGE}"),
            ));
        }
    };
    match run(&options) {
        Ok(receipt) => {
            emit_receipt(&receipt);
            ExitCode::SUCCESS
        }
        Err(failure) => emit_error(&failure),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
