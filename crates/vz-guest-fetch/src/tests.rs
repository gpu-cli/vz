#![allow(clippy::unwrap_used, clippy::expect_used)]
//! What this binary claims, checked against real handshakes rather than shapes.
//!
//! The live tests below put a real rustls server on a real socket and run the
//! product's own `fetch` against it. That matters more than usual here: the
//! claim this binary exists to support is "the chain was verified", and the
//! only way to know that a client verifies is to present it with a chain it
//! must refuse and watch it refuse.

use super::*;

use std::io::{BufRead, BufReader};
use std::net::TcpListener;

fn argv(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_string()).collect()
}

// -- arguments ---------------------------------------------------------------

/// The whole point of the binary: there is no option that turns verification
/// off, and one offered by a caller is refused by name rather than ignored.
/// If a future edit adds an escape hatch, this is where it has to be argued
/// for.
#[test]
fn there_is_no_option_that_disables_verification() {
    for hatch in [
        "insecure",
        "no-check-certificate",
        "k",
        "skip-verify",
        "allow-untrusted",
    ] {
        assert!(!ALLOWED.contains(&hatch), "--{hatch} must not be accepted");
        assert_eq!(
            Options::parse(&argv(&[&format!("--{hatch}"), "1"]), ALLOWED).unwrap_err(),
            format!("unknown option --{hatch}")
        );
    }
}

#[test]
fn unknown_and_valueless_options_are_refused() {
    assert_eq!(
        Options::parse(&argv(&["--url"]), ALLOWED).unwrap_err(),
        "--url requires a value"
    );
    // The next option is not a value: `--url --ca-file p` would otherwise read
    // "--ca-file" as a URL and then report a missing --ca-file, which describes
    // neither mistake.
    assert_eq!(
        Options::parse(&argv(&["--url", "--ca-file", "p"]), ALLOWED).unwrap_err(),
        "--url requires a value"
    );
    assert_eq!(
        Options::parse(&argv(&["--url", "a", "--url", "b"]), ALLOWED).unwrap_err(),
        "--url given more than once"
    );
    assert_eq!(
        Options::parse(&argv(&["url", "a"]), ALLOWED).unwrap_err(),
        "expected an option, found \"url\""
    );
}

#[test]
fn a_required_option_is_reported_by_name() {
    let options = Options::parse(&argv(&["--ca-file", "/anchor.pem"]), ALLOWED).unwrap();
    assert_eq!(options.required("ca-file"), Ok("/anchor.pem"));
    assert_eq!(
        options.required("url").unwrap_err(),
        "missing required option --url"
    );
    assert_eq!(options.optional("url"), None);
}

// -- the target --------------------------------------------------------------

#[test]
fn only_https_urls_are_fetched() {
    for refused in [
        "http://api.one.test/",
        "https:/api.one.test/",
        "api.one.test",
        "ftp://api.one.test/",
        "",
    ] {
        assert!(
            parse_url(refused).is_err(),
            "{refused:?} must not parse as a fetchable URL"
        );
    }
    // Named separately because a downgrade is the dangerous one: it would
    // report a successful fetch over a path that never handshook.
    assert!(
        parse_url("http://api.one.test/")
            .unwrap_err()
            .contains("only https"),
        "a plaintext URL must be refused for being plaintext"
    );
}

#[test]
fn a_url_becomes_a_host_a_port_and_a_request_line() {
    let plain = parse_url("https://api.one.test/health").unwrap();
    assert_eq!(plain.host, "api.one.test");
    assert_eq!(plain.port, 443);
    assert_eq!(plain.path, "/health");
    assert_eq!(plain.authority(), "api.one.test");

    let ported = parse_url("https://API.One.Test:8443/cgi-bin/peer?q=1").unwrap();
    assert_eq!(ported.host, "api.one.test");
    assert_eq!(ported.port, 8443);
    assert_eq!(ported.path, "/cgi-bin/peer?q=1");
    // A non-default port belongs in the Host header, or an origin serving by
    // name is told a name it does not recognise.
    assert_eq!(ported.authority(), "api.one.test:8443");

    assert_eq!(parse_url("https://api.one.test").unwrap().path, "/");
    assert!(parse_url("https://user:pw@api.one.test/").is_err());
    assert!(parse_url("https://api.one.test:0/").is_err());
    assert!(parse_url("https://api.one.test:https/").is_err());
    assert!(parse_url("https://api.one.test/x#frag").is_err());
}

// -- trust -------------------------------------------------------------------

#[test]
fn the_default_trust_bundle_is_the_one_the_image_pins() {
    // `linux/ca-trust/install.sh` writes exactly this path. If the image moves
    // it, this client stops having a trust store and every fetch fails closed
    // -- which is the right failure, but it must be noticed here.
    assert_eq!(DEFAULT_TRUST_BUNDLE, "/etc/vz/ca-certificates.crt");
}

#[test]
fn a_trust_bundle_with_no_anchors_is_refused_rather_than_treated_as_permissive() {
    let dir = std::env::temp_dir().join(format!("vz-guest-fetch-trust-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let empty = dir.join("empty.pem");
    std::fs::write(&empty, b"# no anchors here\n").unwrap();
    let failure = load_trust(empty.to_str().unwrap()).unwrap_err();
    assert_eq!(failure.reason, Reason::TrustBundleEmpty, "{failure:?}");

    let missing = dir.join("absent.pem");
    let failure = load_trust(missing.to_str().unwrap()).unwrap_err();
    assert_eq!(failure.reason, Reason::TrustBundleUnreadable, "{failure:?}");

    let authority = Authority::mint();
    let real = dir.join("authority.pem");
    std::fs::write(&real, authority.pem.as_bytes()).unwrap();
    let (_roots, anchors) = load_trust(real.to_str().unwrap()).unwrap();
    assert_eq!(anchors, 1);
    std::fs::remove_dir_all(&dir).ok();
}

// -- HTTP framing ------------------------------------------------------------

#[test]
fn a_response_is_split_on_its_own_framing() {
    let with_length = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello and then some";
    assert_eq!(
        parse_response(with_length).unwrap(),
        (200, b"hello".to_vec())
    );

    let chunked =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n10.0\r\n4\r\n0.2 \r\n0\r\n\r\n";
    assert_eq!(
        parse_response(chunked).unwrap(),
        (200, b"10.00.2 ".to_vec())
    );

    // No framing header at all: the body is whatever arrived before the close.
    let to_eof = b"HTTP/1.1 404 Not Found\r\nServer: busybox\r\n\r\nnope";
    assert_eq!(parse_response(to_eof).unwrap(), (404, b"nope".to_vec()));

    assert_eq!(
        parse_response(b"HTTP/1.1 200 OK\r\n\r\n").unwrap(),
        (200, Vec::new())
    );
    assert_eq!(
        parse_response(b"not http at all\r\n\r\n")
            .unwrap_err()
            .reason,
        Reason::HttpFailed
    );
    assert_eq!(
        parse_response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort")
            .unwrap_err()
            .reason,
        Reason::HttpFailed
    );
    assert_eq!(
        parse_response(b"HTTP/1.1 200 OK").unwrap_err().reason,
        Reason::HttpFailed
    );
}

#[test]
fn a_response_is_only_complete_when_its_framing_says_so() {
    assert!(!complete(
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhel"
    ));
    assert!(complete(
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
    ));
    assert!(!complete(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nhell"
    ));
    assert!(complete(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nhell\r\n0\r\n\r\n"
    ));
    // Without a framing header, only the close ends it.
    assert!(!complete(
        b"HTTP/1.1 200 OK\r\nServer: busybox\r\n\r\nanything"
    ));
    assert!(!complete(b"HTTP/1.1 200"));
}

#[test]
fn chunk_extensions_are_not_read_as_chunk_sizes() {
    assert_eq!(
        decode_chunked(b"5;name=value\r\nedge!\r\n0\r\n\r\n").unwrap(),
        b"edge!".to_vec()
    );
    assert!(decode_chunked(b"zz\r\n").is_err());
    assert!(decode_chunked(b"5\r\nabc").is_err());
}

// -- live TLS ----------------------------------------------------------------

/// A throwaway certificate authority, standing in for the one an Environment
/// mints for its own edge.
struct Authority {
    pem: String,
    certificate: rcgen::Certificate,
    key: rcgen::KeyPair,
}

impl Authority {
    fn mint() -> Self {
        Self::named("vz Environment test edge authority")
    }

    /// Distinct subject names, exactly as `EdgeIdentity::issue` gives each
    /// Environment's authority. Two authorities sharing a subject would be
    /// matched to each other by name and refused for a bad signature instead
    /// of an unknown issuer -- a true refusal, but not the one an Environment
    /// actually produces.
    fn named(common_name: &str) -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        let mut distinguished = rcgen::DistinguishedName::new();
        distinguished.push(rcgen::DnType::CommonName, common_name);
        params.distinguished_name = distinguished;
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key).unwrap();
        Self {
            pem: certificate.pem(),
            certificate,
            key,
        }
    }

    /// A server configuration presenting a leaf this authority issued for
    /// `names`, plus the authority itself.
    fn server_for(&self, names: &[&str]) -> rustls::ServerConfig {
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(
            names.iter().map(|n| (*n).to_string()).collect::<Vec<_>>(),
        )
        .unwrap();
        params.is_ca = rcgen::IsCa::ExplicitNoCa;
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let leaf = params
            .signed_by(&leaf_key, &self.certificate, &self.key)
            .unwrap();
        let chain = vec![
            rustls_pki_types::CertificateDer::from(leaf.der().to_vec()),
            rustls_pki_types::CertificateDer::from(self.certificate.der().to_vec()),
        ];
        let key = rustls_pki_types::PrivateKeyDer::try_from(leaf_key.serialize_der()).unwrap();
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap()
    }

    fn roots(&self) -> RootCertStore {
        let mut cursor = std::io::Cursor::new(self.pem.clone().into_bytes());
        let mut roots = RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut cursor) {
            roots.add(certificate.unwrap()).unwrap();
        }
        roots
    }
}

/// Serve exactly one HTTPS request on an ephemeral loopback port.
///
/// Deliberately a real socket and a real rustls server rather than an in-memory
/// pair: the failure this binary was written to work around was a record-layer
/// framing disagreement, which only exists on a wire.
fn serve_once(config: rustls::ServerConfig, response: &'static [u8]) -> u16 {
    let listener = TcpListener::bind(("localhost", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let Ok((mut socket, _)) = listener.accept() else {
            return;
        };
        let mut connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
        // A refused client hangs up here; that is the case under test, not a
        // fault, so the handshake failing is not an assertion.
        if connection.complete_io(&mut socket).is_err() {
            return;
        }
        {
            let mut stream = rustls::Stream::new(&mut connection, &mut socket);
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok() {
                if line.ends_with("\r\n\r\n") || line.is_empty() {
                    break;
                }
                if line.ends_with("\r\n") && line.len() == 2 {
                    break;
                }
                line.clear();
            }
            let _ = stream.write_all(response);
            let _ = stream.flush();
        }
        connection.send_close_notify();
        let _ = connection.complete_io(&mut socket);
        let _ = socket.shutdown(std::net::Shutdown::Both);
    });
    port
}

fn target(port: u16, path: &str) -> Target {
    Target {
        host: "localhost".to_string(),
        port,
        path: path.to_string(),
    }
}

const TIMEOUT: Duration = Duration::from_secs(10);

/// The positive case, and the one that says what a pass means: a chain issued
/// by the authority the caller named, for the name the caller asked for.
#[test]
fn a_chain_issued_by_the_named_authority_is_accepted() {
    let authority = Authority::mint();
    let port = serve_once(
        authority.server_for(&["localhost"]),
        b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n10.55.0.1",
    );
    let response = fetch(&target(port, "/cgi-bin/peer"), authority.roots(), TIMEOUT).unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"10.55.0.1".to_vec());
    assert!(response.protocol.starts_with("TLSv1"), "{response:?}");
    assert_eq!(response.peer.port(), port);
}

/// The negative that makes the positive mean something. A different authority
/// signed this chain, so it must be refused -- and refused for that reason,
/// not for some transport accident that would also have "failed".
#[test]
fn a_chain_from_an_unnamed_authority_is_refused_as_an_unknown_issuer() {
    let served_by = Authority::named("vz Environment one edge authority");
    let trusted = Authority::named("vz Environment two edge authority");
    let port = serve_once(
        served_by.server_for(&["localhost"]),
        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret",
    );
    let failure = fetch(&target(port, "/"), trusted.roots(), TIMEOUT).unwrap_err();
    assert_eq!(failure.reason, Reason::CertificateRejected, "{failure:?}");
    assert!(
        failure.detail.contains("UnknownIssuer"),
        "the refusal must name the issuer as the problem: {failure:?}"
    );
}

/// Trusting the right authority is not enough: the certificate has to be for
/// the name that was asked for. Without this, an Environment's edge could
/// answer for any of its names on any of them.
#[test]
fn a_chain_for_another_name_is_refused_even_from_the_named_authority() {
    let authority = Authority::mint();
    let port = serve_once(
        authority.server_for(&["api.two.test"]),
        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecret",
    );
    let failure = fetch(&target(port, "/"), authority.roots(), TIMEOUT).unwrap_err();
    assert_eq!(failure.reason, Reason::CertificateRejected, "{failure:?}");
    assert!(
        failure.detail.contains("NotValidForName"),
        "the refusal must name the hostname as the problem: {failure:?}"
    );
}

/// An empty trust store is empty, not permissive. A caller that pointed at a
/// bundle holding nothing must not get a connection out of it.
#[test]
fn an_empty_trust_store_accepts_nothing() {
    let authority = Authority::mint();
    let port = serve_once(
        authority.server_for(&["localhost"]),
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi",
    );
    let failure = fetch(&target(port, "/"), RootCertStore::empty(), TIMEOUT).unwrap_err();
    assert_eq!(failure.reason, Reason::CertificateRejected, "{failure:?}");
}

/// A chunked origin -- BusyBox `httpd` serves CGI output without a
/// Content-Length -- is read to its terminal chunk rather than to a deadline.
#[test]
fn a_chunked_body_is_decoded() {
    let authority = Authority::mint();
    let port = serve_once(
        authority.server_for(&["localhost"]),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n9\r\n10.55.0.1\r\n0\r\n\r\n",
    );
    let response = fetch(&target(port, "/"), authority.roots(), TIMEOUT).unwrap();
    assert_eq!(response.body, b"10.55.0.1".to_vec());
}

/// A status the origin reported is passed through rather than turned into a
/// failure: the check asserts on it, and a client that collapsed 404 into
/// "fetch failed" would hide which half of the path was wrong.
#[test]
fn a_non_success_status_is_reported_not_swallowed() {
    let authority = Authority::mint();
    let port = serve_once(
        authority.server_for(&["localhost"]),
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n\r\ndown",
    );
    let response = fetch(&target(port, "/"), authority.roots(), TIMEOUT).unwrap();
    assert_eq!(response.status, 503);
    assert_eq!(response.body, b"down".to_vec());
}

/// Nothing listening is a connect failure and says so. This is the reason the
/// negative TLS assertions can be trusted: they check for
/// `certificate_rejected` specifically, and this is what everything else looks
/// like.
#[test]
fn an_absent_origin_is_a_connect_failure_and_not_a_certificate_one() {
    let listener = TcpListener::bind(("localhost", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let authority = Authority::mint();
    let failure = fetch(&target(port, "/"), authority.roots(), TIMEOUT).unwrap_err();
    assert_eq!(failure.reason, Reason::ConnectFailed, "{failure:?}");
    assert_ne!(
        failure.reason.exit_code(),
        Reason::CertificateRejected.exit_code()
    );
}

#[test]
fn every_reason_has_its_own_exit_status() {
    let reasons = [
        Reason::InvalidArguments,
        Reason::TrustBundleUnreadable,
        Reason::TrustBundleEmpty,
        Reason::ResolveFailed,
        Reason::ConnectFailed,
        Reason::CertificateRejected,
        Reason::TlsFailed,
        Reason::HttpFailed,
        Reason::OutputFailed,
    ];
    let codes: std::collections::BTreeSet<u8> =
        reasons.iter().map(|reason| reason.exit_code()).collect();
    assert_eq!(codes.len(), reasons.len());
    // Zero is success and belongs to nothing else.
    assert!(!codes.contains(&0));
}
