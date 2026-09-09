//! The Environment's own certificate authority, and the certificates its edge
//! presents.
//!
//! An Environment-local `.test` name has no public issuer and must not have
//! one: `.test` is reserved precisely so that it never resolves or validates
//! outside the context that defined it. So the Environment issues for itself.
//! The authority is minted per Environment, lives only as long as that
//! Environment, and signs exactly the names that Environment declared.
//!
//! One certificate per declared name, resolved by SNI, rather than one
//! certificate carrying every name. That is not a packaging preference: a
//! single multi-name certificate would complete a handshake for any name a
//! client cared to send, so an undeclared name would be refused only later, by
//! the routing table, and the criterion's "routed ingress" would be a lookup
//! after a successful TLS session rather than a property of the edge. With a
//! resolver, a name the Environment never declared has no certificate and the
//! handshake ends there.

use std::collections::BTreeMap;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("Environment certificate authority: {0}")]
    Authority(#[from] rcgen::Error),
    #[error("edge TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    #[error("an Environment edge cannot be published without at least one declared name")]
    NoNames,
}

/// The Environment's authority and the server configuration its edge serves.
pub struct EdgeIdentity {
    /// The authority certificate, PEM-encoded.
    ///
    /// This is the only thing a client needs in order to verify the edge, and
    /// it is deliberately the only thing exported: the authority's private key
    /// never leaves this process, so possessing the trust anchor does not
    /// confer the ability to issue under it.
    authority_pem: String,
    server: Arc<rustls::ServerConfig>,
}

impl std::fmt::Debug for EdgeIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EdgeIdentity")
            .field("authority_pem_bytes", &self.authority_pem.len())
            .finish()
    }
}

impl EdgeIdentity {
    /// Mint an authority for one Environment and issue one certificate per
    /// declared name.
    pub fn issue(environment_id: &str, names: &[String]) -> Result<Self, IdentityError> {
        if names.is_empty() {
            return Err(IdentityError::NoNames);
        }
        let authority_key = KeyPair::generate()?;
        let mut authority = CertificateParams::new(Vec::new())?;
        authority.distinguished_name =
            distinguished_name(&format!("vz Environment {environment_id} edge authority"));
        // Path length zero: this authority signs end-entity certificates and
        // may not delegate. An Environment's trust anchor that could mint
        // another authority would be a trust anchor for more than the
        // Environment.
        authority.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        authority.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let authority_certificate = authority.self_signed(&authority_key)?;

        let mut resolved: BTreeMap<String, Arc<CertifiedKey>> = BTreeMap::new();
        for name in names {
            let leaf_key = KeyPair::generate()?;
            let mut leaf = CertificateParams::new(vec![name.clone()])?;
            leaf.distinguished_name = distinguished_name(name);
            leaf.is_ca = IsCa::ExplicitNoCa;
            leaf.key_usages = vec![
                KeyUsagePurpose::DigitalSignature,
                KeyUsagePurpose::KeyEncipherment,
            ];
            leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let leaf_certificate =
                leaf.signed_by(&leaf_key, &authority_certificate, &authority_key)?;
            let chain = vec![
                CertificateDer::from(leaf_certificate.der().to_vec()),
                CertificateDer::from(authority_certificate.der().to_vec()),
            ];
            let key = PrivateKeyDer::try_from(leaf_key.serialize_der())
                .map_err(|reason| rustls::Error::General(reason.to_string()))?;
            let signing = rustls::crypto::ring::sign::any_supported_type(&key)?;
            resolved.insert(
                name.to_ascii_lowercase(),
                Arc::new(CertifiedKey::new(chain, signing)),
            );
        }

        // The provider is named rather than taken from process-global state.
        // A daemon that installed a default provider somewhere else would
        // otherwise decide what an Environment's edge negotiates, and which
        // provider signed an Environment's traffic is not a global setting.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(DeclaredNames { resolved }));
        Ok(Self {
            authority_pem: authority_certificate.pem(),
            server: Arc::new(server),
        })
    }

    pub fn authority_pem(&self) -> &str {
        &self.authority_pem
    }

    pub fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.server)
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name.push(DnType::OrganizationName, "vz Developer Environment");
    name
}

/// The names this Environment declared, and nothing else.
#[derive(Debug)]
struct DeclaredNames {
    resolved: BTreeMap<String, Arc<CertifiedKey>>,
}

impl ResolvesServerCert for DeclaredNames {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        // No SNI is not "any name". A client that sent none has not said which
        // declared service it wants, and the edge does not choose one for it.
        let requested = hello.server_name()?.to_ascii_lowercase();
        self.resolved.get(&requested).map(Arc::clone)
    }
}
