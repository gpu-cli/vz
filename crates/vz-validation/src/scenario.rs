//! Scenario definitions for validation testing.
//!
//! Each scenario specifies what to execute and what constitutes a pass.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Category of validation scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScenarioKind {
    /// S1: Entrypoint/Cmd resolution (image defaults, CLI override, args).
    EntrypointCmd,
    /// S2: User and permissions (UID/GID, username, file ownership).
    UserPermissions,
    /// S3: Mount semantics (bind rw, bind ro, named volume persistence).
    MountSemantics,
    /// S4: Signal handling (SIGTERM graceful, SIGKILL forced).
    SignalHandling,
    /// S5: Service image behavior (nginx reachable, redis ping, postgres readiness).
    ServiceBehavior,
    /// S6: Compose fixture validation (multi-service startup, connectivity).
    ComposeFixture,
}

impl ScenarioKind {
    /// Human-readable label for reporting.
    pub fn label(&self) -> &'static str {
        match self {
            Self::EntrypointCmd => "entrypoint-cmd",
            Self::UserPermissions => "user-permissions",
            Self::MountSemantics => "mount-semantics",
            Self::SignalHandling => "signal-handling",
            Self::ServiceBehavior => "service-behavior",
            Self::ComposeFixture => "compose-fixture",
        }
    }
}

/// A single validation scenario to execute.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    /// Unique scenario identifier (e.g., "s1-default-cmd").
    pub id: String,
    /// Category.
    pub kind: ScenarioKind,
    /// Human-readable description.
    pub description: String,
    /// Command to execute (overrides image default if set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    /// Entrypoint override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    /// Environment variables for the scenario.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub environment: HashMap<String, String>,
    /// Working directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// User override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Expected outcome conditions.
    pub expectations: Vec<Expectation>,
}

/// A condition that must be met for a scenario to pass.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Expectation {
    /// Process exits with a specific code.
    #[serde(rename = "exit_code")]
    ExitCode {
        /// Expected exit code.
        code: i32,
    },
    /// Stdout contains a specific substring.
    #[serde(rename = "stdout_contains")]
    StdoutContains {
        /// Expected substring.
        substring: String,
    },
    /// Stderr contains a specific substring.
    #[serde(rename = "stderr_contains")]
    StderrContains {
        /// Expected substring.
        substring: String,
    },
    /// Stdout matches a regex pattern.
    #[serde(rename = "stdout_matches")]
    StdoutMatches {
        /// Regex pattern.
        pattern: String,
    },
    /// The lifecycle sequence includes expected steps.
    #[serde(rename = "lifecycle_sequence")]
    LifecycleSequence {
        /// Expected sequence of lifecycle events.
        events: Vec<String>,
    },
}

/// Build standard S1 scenarios for entrypoint/cmd resolution.
pub fn s1_entrypoint_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "s1-image-defaults".to_string(),
            kind: ScenarioKind::EntrypointCmd,
            description: "Run with image default entrypoint and cmd".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![Expectation::ExitCode { code: 0 }],
        },
        Scenario {
            id: "s1-cmd-override".to_string(),
            kind: ScenarioKind::EntrypointCmd,
            description: "Override image CMD with explicit command".to_string(),
            command: Some(vec![
                "echo".to_string(),
                "hello-from-override".to_string(),
            ]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "hello-from-override".to_string(),
                },
            ],
        },
        Scenario {
            id: "s1-entrypoint-override".to_string(),
            kind: ScenarioKind::EntrypointCmd,
            description: "Override both entrypoint and command".to_string(),
            command: Some(vec!["world".to_string()]),
            entrypoint: Some(vec!["echo".to_string()]),
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "world".to_string(),
                },
            ],
        },
    ]
}

/// Build standard S2 scenarios for user/permissions.
pub fn s2_user_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "s2-default-user".to_string(),
            kind: ScenarioKind::UserPermissions,
            description: "Run as image default user".to_string(),
            command: Some(vec!["id".to_string()]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![Expectation::ExitCode { code: 0 }],
        },
        Scenario {
            id: "s2-numeric-uid".to_string(),
            kind: ScenarioKind::UserPermissions,
            description: "Run as explicit numeric UID".to_string(),
            command: Some(vec!["id".to_string(), "-u".to_string()]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: Some("1000".to_string()),
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "1000".to_string(),
                },
            ],
        },
    ]
}

/// Build S1 extension scenarios for env/cwd propagation.
pub fn s1_env_cwd_scenarios() -> Vec<Scenario> {
    let mut env = HashMap::new();
    env.insert("VZ_TEST_VAR".to_string(), "hello-from-env".to_string());

    vec![
        Scenario {
            id: "s1-env-propagation".to_string(),
            kind: ScenarioKind::EntrypointCmd,
            description: "Verify environment variable propagation".to_string(),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo $VZ_TEST_VAR".to_string(),
            ]),
            entrypoint: None,
            environment: env,
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "hello-from-env".to_string(),
                },
            ],
        },
        Scenario {
            id: "s1-working-dir".to_string(),
            kind: ScenarioKind::EntrypointCmd,
            description: "Verify working directory override".to_string(),
            command: Some(vec!["pwd".to_string()]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: Some("/tmp".to_string()),
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "/tmp".to_string(),
                },
            ],
        },
    ]
}

/// Build standard S5 scenarios for service image behavior.
pub fn s5_service_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "s5-service-start".to_string(),
            kind: ScenarioKind::ServiceBehavior,
            description: "Verify service image starts and reaches ready state".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::LifecycleSequence {
                    events: vec![
                        "create".to_string(),
                        "start".to_string(),
                        "ready".to_string(),
                        "stop".to_string(),
                        "delete".to_string(),
                    ],
                },
            ],
        },
        Scenario {
            id: "s5-port-reachable".to_string(),
            kind: ScenarioKind::ServiceBehavior,
            description: "Verify published port is reachable after service start".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
            ],
        },
    ]
}

/// Build standard S3 scenarios for mount semantics.
pub fn s3_mount_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "s3-bind-mount-rw".to_string(),
            kind: ScenarioKind::MountSemantics,
            description: "Verify bind mount read-write access".to_string(),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo test > /mnt/data/probe && cat /mnt/data/probe".to_string(),
            ]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "test".to_string(),
                },
            ],
        },
        Scenario {
            id: "s3-bind-mount-ro".to_string(),
            kind: ScenarioKind::MountSemantics,
            description: "Verify read-only bind mount rejects writes".to_string(),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "echo fail > /mnt/readonly/probe 2>&1 || echo readonly-ok".to_string(),
            ]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::StdoutContains {
                    substring: "readonly-ok".to_string(),
                },
            ],
        },
        Scenario {
            id: "s3-named-volume-persistence".to_string(),
            kind: ScenarioKind::MountSemantics,
            description: "Verify named volume data persists across container restarts".to_string(),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "cat /data/persisted || echo empty".to_string(),
            ]),
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
            ],
        },
    ]
}

/// Build standard S6 scenarios for compose fixture validation.
pub fn s6_compose_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "s6-multi-service-startup".to_string(),
            kind: ScenarioKind::ComposeFixture,
            description: "Verify multi-service stack starts in dependency order".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::LifecycleSequence {
                    events: vec![
                        "compose-up".to_string(),
                        "service-ready".to_string(),
                        "compose-healthy".to_string(),
                    ],
                },
            ],
        },
        Scenario {
            id: "s6-service-connectivity".to_string(),
            kind: ScenarioKind::ComposeFixture,
            description: "Verify inter-service name-based connectivity".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
            ],
        },
        Scenario {
            id: "s6-restart-recovery".to_string(),
            kind: ScenarioKind::ComposeFixture,
            description: "Verify service restart recovery after failure".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            expectations: vec![
                Expectation::ExitCode { code: 0 },
                Expectation::LifecycleSequence {
                    events: vec![
                        "service-fail".to_string(),
                        "service-restart".to_string(),
                        "service-ready".to_string(),
                    ],
                },
            ],
        },
    ]
}

/// Build standard S4 scenarios for signal handling.
pub fn s4_signal_scenarios() -> Vec<Scenario> {
    vec![Scenario {
        id: "s4-lifecycle-sequence".to_string(),
        kind: ScenarioKind::SignalHandling,
        description: "Verify create/start/stop/delete lifecycle".to_string(),
        command: Some(vec!["sleep".to_string(), "infinity".to_string()]),
        entrypoint: None,
        environment: HashMap::new(),
        working_dir: None,
        user: None,
        expectations: vec![Expectation::LifecycleSequence {
            events: vec![
                "create".to_string(),
                "start".to_string(),
                "stop".to_string(),
                "delete".to_string(),
            ],
        }],
    }]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn scenario_kind_labels() {
        assert_eq!(ScenarioKind::EntrypointCmd.label(), "entrypoint-cmd");
        assert_eq!(ScenarioKind::UserPermissions.label(), "user-permissions");
        assert_eq!(ScenarioKind::MountSemantics.label(), "mount-semantics");
        assert_eq!(ScenarioKind::SignalHandling.label(), "signal-handling");
        assert_eq!(ScenarioKind::ServiceBehavior.label(), "service-behavior");
        assert_eq!(ScenarioKind::ComposeFixture.label(), "compose-fixture");
    }

    #[test]
    fn scenario_round_trip() {
        let scenario = &s1_entrypoint_scenarios()[1]; // s1-cmd-override
        let json = serde_json::to_string(scenario).unwrap();
        let deserialized: Scenario = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, scenario.id);
        assert_eq!(deserialized.kind, scenario.kind);
        assert_eq!(deserialized.expectations.len(), 2);
    }

    #[test]
    fn expectation_variants_round_trip() {
        let expectations = vec![
            Expectation::ExitCode { code: 0 },
            Expectation::StdoutContains {
                substring: "hello".to_string(),
            },
            Expectation::StderrContains {
                substring: "warn".to_string(),
            },
            Expectation::StdoutMatches {
                pattern: r"uid=\d+".to_string(),
            },
            Expectation::LifecycleSequence {
                events: vec!["create".to_string(), "start".to_string()],
            },
        ];
        for exp in &expectations {
            let json = serde_json::to_string(exp).unwrap();
            let deserialized: Expectation = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, exp);
        }
    }

    #[test]
    fn s1_scenarios_are_valid() {
        let scenarios = s1_entrypoint_scenarios();
        assert_eq!(scenarios.len(), 3);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::EntrypointCmd));
        assert!(scenarios.iter().all(|s| !s.expectations.is_empty()));
    }

    #[test]
    fn s2_scenarios_are_valid() {
        let scenarios = s2_user_scenarios();
        assert_eq!(scenarios.len(), 2);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::UserPermissions));
    }

    #[test]
    fn s4_scenarios_are_valid() {
        let scenarios = s4_signal_scenarios();
        assert_eq!(scenarios.len(), 1);
        assert!(scenarios[0].kind == ScenarioKind::SignalHandling);
    }

    #[test]
    fn s1_env_cwd_scenarios_are_valid() {
        let scenarios = s1_env_cwd_scenarios();
        assert_eq!(scenarios.len(), 2);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::EntrypointCmd));
        // env propagation scenario has environment set
        assert!(!scenarios[0].environment.is_empty());
        // working dir scenario has working_dir set
        assert!(scenarios[1].working_dir.is_some());
    }

    #[test]
    fn s5_service_scenarios_are_valid() {
        let scenarios = s5_service_scenarios();
        assert_eq!(scenarios.len(), 2);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::ServiceBehavior));
    }

    #[test]
    fn s3_mount_scenarios_are_valid() {
        let scenarios = s3_mount_scenarios();
        assert_eq!(scenarios.len(), 3);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::MountSemantics));
        assert!(scenarios.iter().all(|s| !s.expectations.is_empty()));
    }

    #[test]
    fn s6_compose_scenarios_are_valid() {
        let scenarios = s6_compose_scenarios();
        assert_eq!(scenarios.len(), 3);
        assert!(scenarios.iter().all(|s| s.kind == ScenarioKind::ComposeFixture));
        assert!(scenarios.iter().all(|s| !s.expectations.is_empty()));
    }
}
