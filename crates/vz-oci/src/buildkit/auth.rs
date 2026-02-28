use bollard_buildkit_proto::moby::filesync::v1::auth_server::Auth;
use bollard_buildkit_proto::moby::filesync::v1::{
    CredentialsRequest, CredentialsResponse, FetchTokenRequest, FetchTokenResponse,
    GetTokenAuthorityRequest, GetTokenAuthorityResponse, VerifyTokenAuthorityRequest,
    VerifyTokenAuthorityResponse,
};
use docker_credential::{CredentialRetrievalError, DockerCredential, get_credential};
use tonic::{Request, Response, Status};

/// Credential payload resolved for a registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedRegistryCredential {
    UsernamePassword { username: String, password: String },
    IdentityToken { token: String },
}

/// Errors raised by Docker credential resolution.
#[derive(Debug, thiserror::Error)]
pub enum DockerAuthError {
    #[error("failed to resolve docker credential for host '{host}': {source}")]
    CredentialLookup {
        host: String,
        source: CredentialRetrievalError,
    },
}

/// BuildKit auth callback provider backed by host Docker credential config.
#[derive(Debug, Default, Clone)]
pub struct DockerAuthProvider;

impl DockerAuthProvider {
    pub fn new() -> Self {
        Self
    }

    /// Resolve credential for a registry host or server URI.
    pub fn resolve_credentials(
        &self,
        host: &str,
    ) -> Result<Option<ResolvedRegistryCredential>, DockerAuthError> {
        for server in credential_lookup_servers(host) {
            match get_credential(&server) {
                Ok(DockerCredential::UsernamePassword(username, password)) => {
                    return Ok(Some(ResolvedRegistryCredential::UsernamePassword {
                        username,
                        password,
                    }));
                }
                Ok(DockerCredential::IdentityToken(token)) => {
                    return Ok(Some(ResolvedRegistryCredential::IdentityToken { token }));
                }
                Err(error) if is_nonfatal_credential_lookup_error(&error) => continue,
                Err(error) => {
                    return Err(DockerAuthError::CredentialLookup {
                        host: server,
                        source: error,
                    });
                }
            }
        }
        Ok(None)
    }
}

#[tonic::async_trait]
impl Auth for DockerAuthProvider {
    async fn credentials(
        &self,
        request: Request<CredentialsRequest>,
    ) -> Result<Response<CredentialsResponse>, Status> {
        let host = request.into_inner().host;
        let credentials = self
            .resolve_credentials(&host)
            .map_err(|error| Status::internal(error.to_string()))?;

        let response = match credentials {
            Some(ResolvedRegistryCredential::UsernamePassword { username, password }) => {
                CredentialsResponse {
                    username,
                    secret: password,
                }
            }
            Some(ResolvedRegistryCredential::IdentityToken { token }) => CredentialsResponse {
                username: "<token>".to_string(),
                secret: token,
            },
            None => CredentialsResponse {
                username: String::new(),
                secret: String::new(),
            },
        };
        Ok(Response::new(response))
    }

    async fn fetch_token(
        &self,
        request: Request<FetchTokenRequest>,
    ) -> Result<Response<FetchTokenResponse>, Status> {
        let host = request.into_inner().host;
        let credentials = self
            .resolve_credentials(&host)
            .map_err(|error| Status::internal(error.to_string()))?;

        let response = match credentials {
            Some(ResolvedRegistryCredential::IdentityToken { token }) => FetchTokenResponse {
                token,
                expires_in: 0,
                issued_at: 0,
            },
            _ => FetchTokenResponse {
                token: String::new(),
                expires_in: 0,
                issued_at: 0,
            },
        };
        Ok(Response::new(response))
    }

    async fn get_token_authority(
        &self,
        _request: Request<GetTokenAuthorityRequest>,
    ) -> Result<Response<GetTokenAuthorityResponse>, Status> {
        Ok(Response::new(GetTokenAuthorityResponse {
            public_key: Vec::new(),
        }))
    }

    async fn verify_token_authority(
        &self,
        _request: Request<VerifyTokenAuthorityRequest>,
    ) -> Result<Response<VerifyTokenAuthorityResponse>, Status> {
        Ok(Response::new(VerifyTokenAuthorityResponse {
            signed: Vec::new(),
        }))
    }
}

fn normalize_registry_host(host: &str) -> String {
    let trimmed = host.trim();
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .to_string()
}

fn credential_lookup_servers(host: &str) -> Vec<String> {
    let normalized = normalize_registry_host(host);
    if is_docker_hub_registry(&normalized) {
        vec![
            "https://index.docker.io/v1/".to_string(),
            "docker.io".to_string(),
            "index.docker.io".to_string(),
            "registry-1.docker.io".to_string(),
        ]
    } else {
        vec![normalized]
    }
}

fn is_docker_hub_registry(registry: &str) -> bool {
    matches!(
        registry,
        "docker.io" | "index.docker.io" | "registry-1.docker.io"
    )
}

fn is_nonfatal_credential_lookup_error(error: &CredentialRetrievalError) -> bool {
    match error {
        CredentialRetrievalError::NoCredentialConfigured
        | CredentialRetrievalError::ConfigNotFound
        | CredentialRetrievalError::ConfigReadError => true,
        CredentialRetrievalError::HelperFailure { stdout, stderr, .. } => {
            let text = format!("{stdout}\n{stderr}").to_ascii_lowercase();
            text.contains("not found")
                || text.contains("credentials not found")
                || text.contains("no credentials")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{credential_lookup_servers, normalize_registry_host};

    #[test]
    fn normalize_registry_host_removes_scheme_and_path() {
        assert_eq!(
            normalize_registry_host("https://ghcr.io/v2/token"),
            "ghcr.io".to_string()
        );
        assert_eq!(normalize_registry_host("docker.io/library"), "docker.io");
    }

    #[test]
    fn docker_hub_hosts_expand_lookup_keys() {
        let keys = credential_lookup_servers("registry-1.docker.io");
        assert!(keys.contains(&"https://index.docker.io/v1/".to_string()));
        assert!(keys.contains(&"docker.io".to_string()));
        assert!(keys.contains(&"index.docker.io".to_string()));
    }
}
