//! Fail-closed rejection for command families removed from the public CLI.
//!
//! Removed roots are detected before clap parsing so neither root nor nested
//! `--help` can revive a hidden compatibility parser. The returned envelope is
//! deliberately independent of daemon or topology state.

use std::ffi::{OsStr, OsString};

use serde::Serialize;

/// Stable process exit code for a removed legacy command.
pub const LEGACY_COMMAND_REMOVED_EXIT_CODE: i32 = 2;

/// Stable machine-readable error code for a removed legacy command.
pub const LEGACY_COMMAND_REMOVED_CODE: &str = "legacy_command_removed";

const ROOT_MIGRATION: &str = "Declare Developer Environment topology in vz.json. Use vz up to create it, vz status to inspect it, vz exec for Machine execution, vz stop to preserve it, and vz delete to remove owned state. Consult installed help for implemented DEV capabilities.";
const BARE_FLAG_MIGRATION: &str = "The implicit sandbox mode was removed. Declare Developer Environment configuration in vz.json. The 0.4 public CLI is converging on explicit vz up, vz exec, vz status, vz stop, and vz delete lifecycle verbs.";
const TYPED_API_MIGRATION: &str =
    "Use the topology-scoped typed API for operations outside the five lifecycle verbs.";

/// Frozen pre-0.4 root command inventory, including the hidden debug family.
/// None of these names has a clap parser or a dispatch path.
pub const REMOVED_ROOT_COMMANDS: &[&str] = &[
    "create",
    "ls",
    "rm",
    "inspect",
    "attach",
    "close-shell",
    "init",
    "run",
    "logs",
    "stack",
    "image",
    "diff",
    "checkpoint",
    "vm",
    "self-sign",
    "debug",
];

/// A removed root command recognized before the active clap command tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemovedRootCommand(&'static str);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovedCliEntry {
    Root(RemovedRootCommand),
    Flag(&'static str),
}

impl RemovedCliEntry {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Root(command) => command.as_str(),
            Self::Flag(flag) => flag,
        }
    }

    const fn migration(self) -> &'static str {
        match self {
            Self::Root(command) => command.migration(),
            Self::Flag(_) => BARE_FLAG_MIGRATION,
        }
    }
}

impl RemovedRootCommand {
    /// Removed root spelling.
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    const fn migration(self) -> &'static str {
        ROOT_MIGRATION
    }

    fn from_root(root: &OsStr) -> Option<Self> {
        let root = root.to_str()?;
        REMOVED_ROOT_COMMANDS
            .iter()
            .find(|candidate| **candidate == root)
            .map(|candidate| Self(candidate))
    }
}

/// Deterministic structured rejection for one removed root command or flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyCommandRejection {
    /// Stable error payload.
    pub error: LegacyCommandError,
}

/// Stable fields describing how to migrate from a removed command or flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyCommandError {
    /// Stable machine-readable error code.
    pub code: &'static str,
    /// Removed root command or flag without the `vz` prefix.
    pub command: &'static str,
    /// Human-readable summary.
    pub message: String,
    /// Replacement lifecycle guidance.
    pub migration: &'static str,
    /// Guidance for functionality intentionally absent from the public CLI.
    pub typed_api_migration: &'static str,
}

impl LegacyCommandRejection {
    fn new(entry: RemovedCliEntry) -> Self {
        Self {
            error: LegacyCommandError {
                code: LEGACY_COMMAND_REMOVED_CODE,
                command: entry.as_str(),
                message: format!(
                    "`vz {}` was removed from the 0.4 public CLI",
                    entry.as_str()
                ),
                migration: entry.migration(),
                typed_api_migration: TYPED_API_MIGRATION,
            },
        }
    }

    /// Render the stable one-line JSON form written to stderr.
    pub fn to_json(&self) -> String {
        // This structure contains no fallible user-derived JSON values. Keep a
        // deterministic structured fallback in case serde itself fails.
        serde_json::to_string(self).unwrap_or_else(|_| {
            concat!(
                "{\"error\":{\"code\":\"legacy_command_removed\",",
                "\"command\":\"unknown\",",
                "\"message\":\"a legacy command was removed from the 0.4 public CLI\",",
                "\"migration\":\"Use the five lifecycle verbs.\",",
                "\"typed_api_migration\":\"Use the topology-scoped typed API.\"}}"
            )
            .to_string()
        })
    }
}

/// Inspect argv and return a rejection when a root command or flag was removed.
///
/// Removed root flags are recognized until an active command or `--` boundary.
/// The root command token and clap's generated `help <command>` traversal are
/// also recognized. Once an active root is found, none of its arguments are
/// inspected.
pub fn rejection_for_args<I, S>(args: I) -> Option<LegacyCommandRejection>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut args = args.into_iter().map(Into::into);
    let _program = args.next();
    let mut options_ended = false;

    while let Some(argument) = args.next() {
        let text = argument.to_str()?;
        if !options_ended && text == "--" {
            // Clap still accepts a subcommand after a root-level `--`. Keep
            // looking for that one root token, but never scan past an active
            // root (for example, `vz exec -- stack`).
            options_ended = true;
            continue;
        }
        if !options_ended && text.starts_with("--") {
            if let Some(flag) = removed_long_flag(text) {
                return Some(LegacyCommandRejection::new(RemovedCliEntry::Flag(flag)));
            }
            continue;
        }
        if !options_ended && text.starts_with('-') && text != "-" {
            if let Some(flag) = removed_short_flag(text) {
                return Some(LegacyCommandRejection::new(RemovedCliEntry::Flag(flag)));
            }
            continue;
        }

        if let Some(command) = RemovedRootCommand::from_root(&argument) {
            return Some(LegacyCommandRejection::new(RemovedCliEntry::Root(command)));
        }

        if text == "help" {
            return rejection_for_help_path(args);
        }

        return None;
    }

    None
}

fn rejection_for_help_path(args: impl Iterator<Item = OsString>) -> Option<LegacyCommandRejection> {
    let mut options_ended = false;

    for argument in args {
        let text = argument.to_str()?;

        if !options_ended && text == "--" {
            options_ended = true;
            continue;
        }
        if !options_ended && text.starts_with("--") {
            if let Some(flag) = removed_long_flag(text) {
                return Some(LegacyCommandRejection::new(RemovedCliEntry::Flag(flag)));
            }
            continue;
        }
        if !options_ended && text.starts_with('-') && text != "-" {
            if let Some(flag) = removed_short_flag(text) {
                return Some(LegacyCommandRejection::new(RemovedCliEntry::Flag(flag)));
            }
            continue;
        }

        return RemovedRootCommand::from_root(&argument)
            .map(RemovedCliEntry::Root)
            .map(LegacyCommandRejection::new);
    }

    None
}

fn removed_long_flag(option: &str) -> Option<&'static str> {
    match option.split_once('=').map_or(option, |(name, _)| name) {
        "--continue" => Some("--continue"),
        "--resume" => Some("--resume"),
        "--name" => Some("--name"),
        "--ephemeral" => Some("--ephemeral"),
        "--cpus" => Some("--cpus"),
        "--memory" => Some("--memory"),
        "--base-image" => Some("--base-image"),
        "--main-container" => Some("--main-container"),
        "--control-plane" => Some("--control-plane"),
        _ => None,
    }
}

fn removed_short_flag(option: &str) -> Option<&'static str> {
    let cluster = option.strip_prefix('-')?;
    for flag in cluster.chars() {
        match flag {
            'c' => return Some("-c"),
            'r' => return Some("-r"),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_removed_root_through_supported_global_options() {
        for args in [
            vec!["vz", "stack"],
            vec!["vz", "stack", "up"],
            vec!["vz", "--json", "stack", "--help"],
            vec!["vz", "-vq", "stack"],
            vec!["vz", "--", "stack"],
            vec!["vz", "help", "stack"],
            vec!["vz", "help", "stack", "up"],
            vec!["vz", "help", "stack", "--help"],
            vec!["vz", "help", "--", "stack"],
        ] {
            let rejection = rejection_for_args(args).expect("stack must be rejected");
            assert_eq!(rejection.error.code, LEGACY_COMMAND_REMOVED_CODE);
            assert_eq!(rejection.error.command, "stack");
        }
    }

    #[test]
    fn explicit_command_arguments_and_tokens_after_double_dash_are_not_reinterpreted() {
        for args in [
            vec!["vz", "exec", "--", "stack"],
            vec!["vz", "exec", "--", "--resume"],
            vec!["vz", "help", "status", "stack"],
            vec!["vz", "--", "--name", "stack"],
        ] {
            assert!(rejection_for_args(args).is_none());
        }
    }

    #[test]
    fn recognizes_every_removed_bare_mode_flag_and_alias() {
        for (args, expected_flag) in [
            (vec!["vz", "-c"], "-c"),
            (vec!["vz", "-hc"], "-c"),
            (vec!["vz", "-vc"], "-c"),
            (vec!["vz", "-qc"], "-c"),
            (vec!["vz", "--continue"], "--continue"),
            (vec!["vz", "-r", "target"], "-r"),
            (vec!["vz", "-vrtarget"], "-r"),
            (vec!["vz", "-vrcandidate"], "-r"),
            (vec!["vz", "-Vcr"], "-c"),
            (vec!["vz", "--resume=target"], "--resume"),
            (vec!["vz", "--name", "target"], "--name"),
            (vec!["vz", "--ephemeral"], "--ephemeral"),
            (vec!["vz", "--cpus", "4"], "--cpus"),
            (vec!["vz", "--memory=4096"], "--memory"),
            (vec!["vz", "--base-image", "alpine"], "--base-image"),
            (vec!["vz", "--main-container=app"], "--main-container"),
            (
                vec!["vz", "--control-plane", "daemon-grpc"],
                "--control-plane",
            ),
        ] {
            let rejection = rejection_for_args(args).expect("flag must be rejected");
            assert_eq!(rejection.error.command, expected_flag);
            assert_eq!(rejection.error.migration, BARE_FLAG_MIGRATION);
        }
    }

    #[test]
    fn recognizes_removed_flag_in_generated_help_traversal() {
        let rejection = rejection_for_args(["vz", "help", "--name", "stack"]).unwrap();
        assert_eq!(rejection.error.command, "--name");
    }

    #[test]
    fn every_removed_root_rejects_every_nested_argument_and_help_route() {
        for root in REMOVED_ROOT_COMMANDS {
            for prefix in [
                vec!["vz"],
                vec!["vz", "--json"],
                vec!["vz", "-vvq"],
                vec!["vz", "--"],
                vec!["vz", "help"],
                vec!["vz", "--help"],
                vec!["vz", "--version"],
                vec!["vz", "help", "--"],
                vec!["vz", "help", "-v"],
            ] {
                for suffix in [
                    vec![],
                    vec!["--help"],
                    vec!["--", "arbitrary"],
                    vec!["unknown", "--arbitrary", "value"],
                ] {
                    let args = prefix
                        .iter()
                        .copied()
                        .chain(std::iter::once(*root))
                        .chain(suffix.iter().copied());
                    let rejection = rejection_for_args(args).expect("legacy root must reject");
                    assert_eq!(rejection.error.command, *root);
                    assert_eq!(rejection.error.code, LEGACY_COMMAND_REMOVED_CODE);
                    assert_eq!(rejection.error.migration, ROOT_MIGRATION);
                }
            }
        }
    }

    #[test]
    fn active_exec_arguments_remain_opaque_even_if_legacy_spellings() {
        for root in REMOVED_ROOT_COMMANDS {
            assert!(rejection_for_args(["vz", "exec", "--", root, "--resume"]).is_none());
            assert!(rejection_for_args(["vz", "status", "--environment", root]).is_none());
            assert!(rejection_for_args(["vz", "stop", "--environment", root]).is_none());
        }
    }

    #[test]
    fn renders_deterministic_structured_migration_error() {
        let rejection = rejection_for_args(["vz", "stack", "up"]).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rejection.to_json()).unwrap();
        assert_eq!(value["error"]["code"], LEGACY_COMMAND_REMOVED_CODE);
        assert_eq!(value["error"]["command"], "stack");
        assert_eq!(
            value["error"]["message"],
            "`vz stack` was removed from the 0.4 public CLI"
        );
        assert_eq!(value["error"]["migration"], ROOT_MIGRATION);
        assert_eq!(value["error"]["typed_api_migration"], TYPED_API_MIGRATION);
        assert_eq!(rejection.to_json(), rejection.to_json());
        assert_eq!(rejection.to_json().lines().count(), 1);
    }
}
