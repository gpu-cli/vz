//! Docker Engine facade artifact provisioning.
//!
//! The facade is deliberately downstream of vz's native OCI runtime. This
//! module only provisions the daemon-side binaries required by Docker Engine;
//! it does not install the Docker CLI or an OCI runtime.

mod artifacts;

pub use artifacts::{
    DOCKER_ENGINE_VERSION, DockerArtifactError, DockerArtifacts, DockerVersionMetadata,
    ensure_docker_artifacts,
};
