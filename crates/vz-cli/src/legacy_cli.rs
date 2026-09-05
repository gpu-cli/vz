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

const STACK_MIGRATION: &str = "Declare services and Machines in vz.json. The 0.4 public CLI is converging on five lifecycle verbs: vz up, vz exec, vz status, vz stop, and vz delete.";
const TYPED_API_MIGRATION: &str =
    "Use the topology-scoped typed API for operations outside the five lifecycle verbs.";

/// A removed root command recognized before the active clap command tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovedRootCommand {
    /// The pre-0.4 Compose/stack command family.
    Stack,
}

impl RemovedRootCommand {
    /// Removed root spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stack => "stack",
        }
    }

    const fn migration(self) -> &'static str {
        match self {
            Self::Stack => STACK_MIGRATION,
        }
    }

    fn from_root(root: &OsStr) -> Option<Self> {
        match root.to_str() {
            Some("stack") => Some(Self::Stack),
            _ => None,
        }
    }
}

/// Deterministic structured rejection for one removed root command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyCommandRejection {
    /// Stable error payload.
    pub error: LegacyCommandError,
}

/// Stable fields describing how to migrate from a removed command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LegacyCommandError {
    /// Stable machine-readable error code.
    pub code: &'static str,
    /// Removed root command without the `vz` prefix.
    pub command: &'static str,
    /// Human-readable summary.
    pub message: String,
    /// Replacement lifecycle guidance.
    pub migration: &'static str,
    /// Guidance for functionality intentionally absent from the public CLI.
    pub typed_api_migration: &'static str,
}

impl LegacyCommandRejection {
    fn new(command: RemovedRootCommand) -> Self {
        Self {
            error: LegacyCommandError {
                code: LEGACY_COMMAND_REMOVED_CODE,
                command: command.as_str(),
                message: format!(
                    "`vz {}` was removed from the 0.4 public CLI",
                    command.as_str()
                ),
                migration: command.migration(),
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

/// Inspect argv and return a rejection when its root command was removed.
///
/// Only the root command token is considered, except that clap's generated
/// `help <command>` traversal is also recognized. Values belonging to global
/// options are skipped so a value equal to `stack` cannot be mistaken for a
/// command. Once an active root is found, none of its arguments are inspected.
pub fn rejection_for_args<I, S>(args: I) -> Option<LegacyCommandRejection>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut args = args.into_iter().map(Into::into);
    let _program = args.next();
    let mut consume_next_as_option_value = false;
    let mut options_ended = false;

    while let Some(argument) = args.next() {
        if consume_next_as_option_value {
            consume_next_as_option_value = false;
            continue;
        }

        let text = argument.to_str()?;
        if !options_ended && text == "--" {
            // Clap still accepts a subcommand after a root-level `--`. Keep
            // looking for that one root token, but never scan past an active
            // root (for example, `vz run -- stack`).
            options_ended = true;
            continue;
        }
        if !options_ended && text.starts_with("--") {
            if !text.contains('=') && long_option_takes_value(text) {
                consume_next_as_option_value = true;
            }
            continue;
        }
        if !options_ended && text.starts_with('-') && text != "-" {
            if short_option_takes_separate_value(text) {
                consume_next_as_option_value = true;
            }
            continue;
        }

        if let Some(command) = RemovedRootCommand::from_root(&argument) {
            return Some(LegacyCommandRejection::new(command));
        }

        if text == "help" {
            return rejection_for_help_path(args);
        }

        return None;
    }

    None
}

fn rejection_for_help_path(args: impl Iterator<Item = OsString>) -> Option<LegacyCommandRejection> {
    let mut consume_next_as_option_value = false;
    let mut options_ended = false;

    for argument in args {
        if consume_next_as_option_value {
            consume_next_as_option_value = false;
            continue;
        }

        let text = argument.to_str()?;

        if !options_ended && text == "--" {
            options_ended = true;
            continue;
        }
        if !options_ended && text.starts_with("--") {
            if !text.contains('=') && long_option_takes_value(text) {
                consume_next_as_option_value = true;
            }
            continue;
        }
        if !options_ended && text.starts_with('-') && text != "-" {
            if short_option_takes_separate_value(text) {
                consume_next_as_option_value = true;
            }
            continue;
        }

        return RemovedRootCommand::from_root(&argument).map(LegacyCommandRejection::new);
    }

    None
}

fn long_option_takes_value(option: &str) -> bool {
    matches!(
        option,
        "--control-plane"
            | "--resume"
            | "--name"
            | "--cpus"
            | "--memory"
            | "--base-image"
            | "--main-container"
    )
}

fn short_option_takes_separate_value(option: &str) -> bool {
    let Some(cluster) = option.strip_prefix('-') else {
        return false;
    };
    let mut flags = cluster.chars().peekable();

    while let Some(flag) = flags.next() {
        match flag {
            'v' | 'q' | 'c' => {}
            'r' => return flags.peek().is_none(),
            _ => return false,
        }
    }

    false
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
            vec!["vz", "--control-plane=daemon-grpc", "stack"],
            vec!["vz", "--control-plane", "daemon-grpc", "stack", "unknown"],
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
    fn does_not_reinterpret_option_values_or_command_arguments_as_roots() {
        for args in [
            vec!["vz", "--name", "stack"],
            vec!["vz", "-r", "stack"],
            vec!["vz", "-vr", "stack"],
            vec!["vz", "-qr", "stack"],
            vec!["vz", "-vqr", "stack"],
            vec!["vz", "-rstack"],
            vec!["vz", "run", "--", "stack"],
            vec!["vz", "exec", "--", "stack"],
            vec!["vz", "help", "image", "stack"],
            vec!["vz", "help", "--name", "stack"],
            vec!["vz", "--", "--name", "stack"],
        ] {
            assert!(rejection_for_args(args).is_none());
        }
    }

    #[test]
    fn recognizes_removed_root_after_inline_short_option_values() {
        for args in [
            vec!["vz", "-rresume-target", "stack"],
            vec!["vz", "-vrresume-target", "stack"],
        ] {
            assert!(rejection_for_args(args).is_some());
        }
    }

    #[test]
    fn renders_deterministic_structured_migration_error() {
        let rejection = rejection_for_args(["vz", "stack", "up"]).unwrap();
        assert_eq!(
            rejection.to_json(),
            concat!(
                "{\"error\":{\"code\":\"legacy_command_removed\",",
                "\"command\":\"stack\",",
                "\"message\":\"`vz stack` was removed from the 0.4 public CLI\",",
                "\"migration\":\"Declare services and Machines in vz.json. The 0.4 public CLI is converging on five lifecycle verbs: vz up, vz exec, vz status, vz stop, and vz delete.\",",
                "\"typed_api_migration\":\"Use the topology-scoped typed API for operations outside the five lifecycle verbs.\"}}"
            )
        );
    }
}
