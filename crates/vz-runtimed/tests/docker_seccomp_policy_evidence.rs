//! Offline command-double checks for the physical helper's failure/cleanup paths.
#![allow(clippy::unwrap_used)]

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{cell::RefCell, path::Path};

#[path = "support/docker_namespace_values.rs"]
#[allow(dead_code)]
mod docker_namespace_values;
#[path = "support/docker_seccomp_policy.rs"]
#[allow(dead_code)] // Compile the physical entry point; no VM is run here.
mod proof;

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    LostExecProfile,
    WrongProfile,
    RunTransport,
    MissingCidfile,
    CleanupMismatch,
}

struct State {
    tenant: bool,
    custom: bool,
    fault: Fault,
    cidfile: String,
    calls: Vec<Vec<String>>,
}

tokio::task_local! { static STATE: RefCell<State>; }

fn output(exit_code: i32, stdout: &str) -> Value {
    json!({"exit_code": exit_code, "stdout": stdout, "stderr": ""})
}

async fn host_docker(_: &Path, _: &Path, args: &[&str], input: Vec<u8>) -> Result<Value> {
    ensure!(input.is_empty());
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        state
            .calls
            .push(args.iter().map(|arg| (*arg).to_owned()).collect());
        let id = "a".repeat(64);
        match args[0] {
            "run" => {
                let at = args
                    .iter()
                    .position(|arg| *arg == "--cidfile")
                    .context("missing cidfile")?;
                state.cidfile = args[at + 1].to_owned();
                if state.fault != Fault::MissingCidfile {
                    std::fs::write(&state.cidfile, &id)?;
                }
                if state.fault == Fault::RunTransport {
                    anyhow::bail!("injected run transport error");
                }
                Ok(if state.tenant {
                    output(0, &(id + "\n"))
                } else if state.custom {
                    output(1, "")
                } else {
                    output(0, "/bin/busybox\n")
                })
            }
            "exec" => Ok(if state.custom && state.fault != Fault::LostExecProfile {
                output(1, "")
            } else {
                output(0, "/bin/busybox\n")
            }),
            "inspect" => {
                let security = if state.fault == Fault::WrongProfile {
                    json!(["seccomp=unconfined"])
                } else if state.custom {
                    json!([format!("seccomp={}", proof::PROFILE)])
                } else {
                    Value::Null
                };
                Ok(output(
                    0,
                    &json!({"id": id, "state": {"Running": state.tenant,
                    "ExitCode": if state.custom && !state.tenant { 1 } else { 0 },
                    "Error": "", "Status": if state.tenant { "running" } else { "exited" },
                    "Dead": false, "Paused": false, "Restarting": false, "OOMKilled": false,
                    "Pid": if state.tenant { 321 } else { 0 }},
                    "host_config": {"Runtime": "youki", "Privileged": false, "NetworkMode": "none",
                        "ContainerIDFile": state.cidfile, "SecurityOpt": security,
                        "CapAdd": null, "CapDrop": null, "Devices": [], "DeviceCgroupRules": null}})
                    .to_string(),
                ))
            }
            "rm" => Ok(output(
                0,
                &(if state.fault == Fault::CleanupMismatch {
                    "b".repeat(64)
                } else {
                    id
                } + "\n"),
            )),
            _ => anyhow::bail!("unexpected command"),
        }
    })
}

fn docker_stdout(value: &Value) -> Result<&str> {
    ensure!(value["exit_code"] == 0, "command failed: {value}");
    value["stdout"].as_str().context("stdout")
}

async fn execute(tenant: bool, custom: bool, fault: Fault) -> (Result<Value>, Vec<Vec<String>>) {
    let directory = tempfile::tempdir().unwrap();
    STATE
        .scope(
            RefCell::new(State {
                tenant,
                custom,
                fault,
                cidfile: String::new(),
                calls: vec![],
            }),
            async {
                let result = proof::case(
                    Path::new("/exact-machine.sock"),
                    directory.path(),
                    tenant,
                    custom,
                )
                .await;
                STATE.with(|state| (result, state.borrow().calls.clone()))
            },
        )
        .await
}

fn cleaned(calls: &[Vec<String>]) {
    assert_eq!(
        calls.last(),
        Some(&vec!["rm".to_owned(), "-f".to_owned(), "a".repeat(64)])
    );
}

#[tokio::test]
async fn all_four_syscall_cases_preserve_raw_receipts_and_cleanup() {
    for tenant in [false, true] {
        for custom in [false, true] {
            let (result, calls) = execute(tenant, custom, Fault::None).await;
            let proof = result.unwrap();
            assert_eq!(proof["cleanup_confirmed"], true);
            assert_eq!(proof["syscall_allowed"], !custom);
            cleaned(&calls);
        }
    }
}

#[tokio::test]
async fn lost_exec_filter_and_wrong_profile_fail_without_skipping_cleanup() {
    for fault in [Fault::LostExecProfile, Fault::WrongProfile] {
        let (result, calls) = execute(true, true, fault).await;
        assert!(result.is_err());
        cleaned(&calls);
    }
}

#[tokio::test]
async fn transport_failure_with_cid_still_cleans_exact_container() {
    let (result, calls) = execute(true, true, Fault::RunTransport).await;
    assert!(result.is_err());
    cleaned(&calls);
}

#[tokio::test]
async fn detached_stdout_id_is_a_safe_cleanup_fallback() {
    let (result, calls) = execute(true, true, Fault::MissingCidfile).await;
    assert!(result.is_ok());
    cleaned(&calls);
    let (result, calls) = execute(false, false, Fault::MissingCidfile).await;
    assert!(result.is_err());
    assert_eq!(calls.len(), 1); // /bin/busybox output is never interpreted as an ID.
}

#[tokio::test]
async fn mismatched_cleanup_receipt_cannot_claim_success() {
    let (result, calls) = execute(true, true, Fault::CleanupMismatch).await;
    assert!(result.is_err());
    cleaned(&calls);
}
