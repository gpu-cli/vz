//! Declared SecretBindings: how a value reaches exactly one Machine.
//!
//! A SecretBinding names a host-side SOURCE, the Machine it is for, and the
//! path it appears at inside that Machine. The value itself never appears here
//! and never enters durable state: the declaration is identity, and the bytes
//! are carried once, at Up, to the one Machine that declared them.
//!
//! Criterion 18 of the 0.4 gate is the contract this serves -- scoped to the
//! selected Environment/Machine, redacted from status/logs/evidence, audited on
//! use, and denied across a boundary.

use serde::{Deserialize, Serialize};

use super::topology::{TopologyValidationError, validate_name};

/// Where a binding's value comes from on the host.
///
/// An environment variable rather than a literal or a file path, because a
/// literal would put the value into the ProjectDefinition -- a checked-in file,
/// and one the gate's own state-root sweep reads back -- and a path would make
/// the Machine's secret depend on host layout the Environment does not own.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SecretBindingSpec {
    pub schema_version: u32,
    /// Topology-local binding name, unique within the Environment.
    pub name: String,
    /// The one Machine this binding is for. A binding with no Machine would be
    /// an Environment-wide secret, which is the opposite of what this is.
    pub machine: String,
    /// Absolute, bounded, `..`-free path inside that Machine where the value
    /// appears.
    pub target_path: String,
    /// Host environment variable the CLI reads the value from.
    ///
    /// Exactly one of `source_env` and `source_command` is required. Siblings
    /// rather than a nested tagged union because the flat spelling is what a
    /// declaration reads like, and because `source_env` predates the command
    /// form and stays valid unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_env: Option<String>,
    /// A host command whose STDOUT is the value, as an argv vector.
    ///
    /// This is how a secret manager is integrated: `op read op://vault/item`,
    /// `vault kv get`, `aws secretsmanager get-secret-value`, `pass show`. vz
    /// does not own any of their CLI surfaces, auth models or version drift --
    /// it runs what the declaration names and reads stdout.
    ///
    /// An argv VECTOR and never a shell string: no shell means no word
    /// splitting, no globbing, and no way for a value or an argument to become
    /// another command.
    ///
    /// SECURITY. A definition is a checked-in file, so a declaration naming a
    /// command is host code execution at `vz up` for anyone who clones the
    /// repository and runs it. That is a materially different exposure from
    /// `source_env`, which only reads an environment the caller already has.
    /// The exact argv is recorded in the audit log on every use, so a
    /// definition that runs something unexpected is visible afterwards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_command: Option<Vec<String>>,
    /// Another Environment's identity, when the declaration asks to bind a
    /// secret that Environment owns.
    ///
    /// Present only to be REFUSED. Separate Environments are default-deny and a
    /// secret is not among the things a directional grant can cross, so this
    /// exists so the refusal can be asked for explicitly and proved, rather
    /// than being unrepresentable and therefore untested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_environment: Option<String>,
}

/// The persisted record of one binding. Identity only.
///
/// There is deliberately no value, no digest of the value, and no source bytes:
/// a digest is a verifier for anyone who already has a guess, and durable state
/// is one of the seven artifact groups criterion 18's redaction sweep reads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretBindingInstance {
    pub schema_version: u32,
    pub binding_id: super::topology::SecretBindingId,
    pub environment_id: super::topology::EnvironmentId,
    pub machine_id: super::topology::MachineId,
    pub name: String,
    pub target_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_command: Option<Vec<String>>,
}

/// The longest a binding's target path or source variable may be.
const MAX_TARGET_PATH: usize = 1024;
const MAX_SOURCE_ENV: usize = 256;
const MAX_COMMAND_ARGS: usize = 64;
const MAX_COMMAND_ARG: usize = 4096;

pub fn validate_secret_binding(spec: &SecretBindingSpec) -> Result<(), TopologyValidationError> {
    validate_name("secret_binding", &spec.name)?;
    validate_name("secret_binding.machine", &spec.machine)?;
    let invalid = |reason: &str| TopologyValidationError::InvalidName {
        kind: "secret_binding.".to_string() + reason,
        value: spec.name.clone(),
    };
    if spec.target_path.is_empty()
        || spec.target_path.len() > MAX_TARGET_PATH
        || !spec.target_path.starts_with('/')
        || spec
            .target_path
            .split('/')
            .any(|segment| segment == ".." || segment == ".")
        || spec.target_path.contains('\0')
    {
        return Err(invalid("target_path"));
    }
    // EXACTLY one source. Neither is a declaration that names no value at all;
    // both is a declaration whose value depends on which one the reader
    // happens to consult first.
    match (&spec.source_env, &spec.source_command) {
        (Some(_), Some(_)) | (None, None) => return Err(invalid("source")),
        _ => {}
    }
    if let Some(source_env) = &spec.source_env {
        // A POSIX-ish environment variable name. Checked rather than assumed
        // because the CLI reads it out of its own environment and a permissive
        // spelling is a way to ask for something that is not a variable at all.
        if source_env.is_empty()
            || source_env.len() > MAX_SOURCE_ENV
            || !source_env
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            || source_env.starts_with(|c: char| c.is_ascii_digit())
        {
            return Err(invalid("source_env"));
        }
    }
    if let Some(source_command) = &spec.source_command {
        // An absolute or bare program name and bounded arguments. No shell is
        // ever involved, so nothing here is parsed: the first element is the
        // program and the rest are arguments verbatim.
        if source_command.is_empty()
            || source_command.len() > MAX_COMMAND_ARGS
            || source_command.iter().any(|argument| {
                argument.is_empty() || argument.len() > MAX_COMMAND_ARG || argument.contains('\0')
            })
        {
            return Err(invalid("source_command"));
        }
    }
    Ok(())
}
