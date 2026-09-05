//! Offline receipt/cleanup tests. The command double never contacts Docker.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

#[path = "support/docker_namespace_values.rs"]
#[allow(dead_code)] // This helper uses only the container-ID parser.
mod docker_namespace_values;

#[path = "support/docker_device_policy.rs"]
#[allow(dead_code)] // Compile-check prove(); only its command cases run with this double.
mod proof;

tokio::task_local! {
    static STATE: RefCell<State>;
}

struct State {
    receipts: VecDeque<Value>,
    calls: Vec<Vec<String>>,
}

async fn host_docker(_: &Path, _: &Path, args: &[&str], input: Vec<u8>) -> Result<Value> {
    ensure!(input.is_empty());
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        state
            .calls
            .push(args.iter().map(|value| (*value).to_owned()).collect());
        let receipt = state.receipts.pop_front().context("unexpected command")?;
        if receipt["write_cidfile"] == true {
            let index = args
                .iter()
                .position(|value| *value == "--cidfile")
                .context("cidfile argument")?;
            std::fs::write(args[index + 1], "a".repeat(64))?;
        }
        if receipt["transport_failure"] == true {
            anyhow::bail!("injected command transport failure");
        }
        Ok(receipt)
    })
}

fn docker_stdout(value: &Value) -> Result<&str> {
    ensure!(value["exit_code"] == 0, "host Docker failed: {value}");
    value["stdout"].as_str().context("stdout")
}

fn receipt(code: i32, stdout: &str, stderr: &str) -> Value {
    json!({"exit_code": code, "stdout": stdout, "stderr": stderr})
}

fn sequence(allow_loop_control: bool) -> Vec<Value> {
    let id = "a".repeat(64);
    vec![
        receipt(0, &format!("{id}\n"), ""),
        receipt(
            0,
            &json!({"id": id, "state": {"Running": true}, "host_config": {
                "Runtime": "youki", "Privileged": false, "NetworkMode": "none",
                "DeviceCgroupRules": if allow_loop_control { json!(["c 10:237 rwm"]) } else { Value::Null },
            }})
            .to_string(),
            "",
        ),
        receipt(0, "CapEff:\t0000000008000000\nSeccomp:\t2\n", ""),
        receipt(0, "CapEff:\t0000000008000000\nSeccomp:\t2\n", ""),
        receipt(
            0,
            "character special file:1:3\ncharacter special file:a:ed\n",
            "",
        ),
        if allow_loop_control {
            receipt(0, "", "0+0 records in\n0+0 records out\n")
        } else {
            receipt(
                1,
                "",
                "dd: can't open '/dev/vz-policy-loop-control': Operation not permitted\n",
            )
        },
        receipt(0, &format!("{id}\n"), ""),
    ]
}

async fn execute(
    receipts: Vec<Value>,
    allow_loop_control: bool,
) -> (Result<Value>, Vec<Vec<String>>) {
    let directory =
        tempfile::tempdir().unwrap_or_else(|error| panic!("fixture directory: {error}"));
    STATE
        .scope(
            RefCell::new(State {
                receipts: receipts.into(),
                calls: vec![],
            }),
            async {
                let result = proof::case(
                    Path::new("/exact-machine.sock"),
                    directory.path(),
                    allow_loop_control,
                )
                .await;
                STATE.with(|state| {
                    let state = state.borrow();
                    assert!(state.receipts.is_empty(), "unconsumed receipts");
                    (result, state.calls.clone())
                })
            },
        )
        .await
}

fn exact_cleanup(calls: &[Vec<String>]) {
    assert_eq!(
        calls.last(),
        Some(&vec!["rm".to_owned(), "-f".to_owned(), "a".repeat(64)])
    );
    for call in calls {
        assert!(!call.iter().any(|arg| arg == "--privileged"
            || arg == "--runtime"
            || arg == "--security-opt"
            || arg == "--cap-add"));
    }
}

#[tokio::test]
async fn paired_cases_preserve_raw_receipts_and_exact_cleanup() {
    for allow in [false, true] {
        let (result, calls) = execute(sequence(allow), allow).await;
        let result = result.unwrap_or_else(|error| panic!("valid proof failed: {error:#}"));
        assert_eq!(result["cleanup_confirmed"], true);
        assert_eq!(result["loop_control_open_allowed"], allow);
        assert_eq!(result["numeric_errno_measured"], false);
        assert!(result["commands"]["loop_control_open"].is_object());
        assert_eq!(result["init_seccomp"], 2);
        assert_eq!(result["exec_seccomp"], 2);
        assert_eq!(result["init_cap_eff"], result["exec_cap_eff"]);
        assert_eq!(calls[2].last().map(String::as_str), Some("/proc/1/status"));
        assert_eq!(
            calls[3].last().map(String::as_str),
            Some("/proc/self/status")
        );
        assert_eq!(
            calls[0].iter().any(|arg| arg == "--device-cgroup-rule"),
            allow
        );
        exact_cleanup(&calls);
    }
}

#[tokio::test]
async fn non_eperm_open_failure_is_not_a_policy_pass_and_still_cleans() {
    let mut receipts = sequence(false);
    receipts[5]["stderr"] = json!("dd: can't open '/dev/vz-policy-loop-control': No such device\n");
    let (result, calls) = execute(receipts, false).await;
    assert!(
        result
            .err()
            .unwrap_or_else(|| panic!("invalid errno proof succeeded"))
            .to_string()
            .contains("exact C-locale EPERM")
    );
    exact_cleanup(&calls);
}

#[tokio::test]
async fn missing_capability_fails_before_device_test_and_still_cleans() {
    let mut receipts = sequence(false);
    receipts[2]["stdout"] = json!("CapEff:\t0000000000000000\nSeccomp:\t2\n");
    receipts.drain(4..6);
    let (result, calls) = execute(receipts, false).await;
    assert!(
        result
            .err()
            .unwrap_or_else(|| panic!("missing capability proof succeeded"))
            .to_string()
            .contains("CAP_MKNOD")
    );
    exact_cleanup(&calls);
}

#[tokio::test]
async fn failed_run_uses_validated_cidfile_for_cleanup() {
    let mut run = receipt(125, "", "runtime failed after creation");
    run["write_cidfile"] = json!(true);
    let (result, calls) = execute(vec![run, receipt(0, &"a".repeat(64), "")], false).await;
    assert!(result.is_err());
    exact_cleanup(&calls);
}

#[tokio::test]
async fn transport_failure_after_creation_still_uses_exact_cleanup() {
    let run = json!({"write_cidfile": true, "transport_failure": true});
    let (result, calls) = execute(vec![run, receipt(0, &"a".repeat(64), "")], false).await;
    assert!(
        result
            .err()
            .unwrap_or_else(|| panic!("failed transport proof succeeded"))
            .to_string()
            .contains("injected command transport failure")
    );
    exact_cleanup(&calls);
}

#[tokio::test]
async fn cleanup_mismatch_prevents_success_and_retains_evidence() {
    let mut receipts = sequence(false);
    receipts[6]["stdout"] = json!("b".repeat(64));
    let (result, calls) = execute(receipts, false).await;
    let error = result
        .err()
        .unwrap_or_else(|| panic!("wrong cleanup proof succeeded"))
        .to_string();
    assert!(error.contains("cleanup ID mismatch") && error.contains("loop_control_open"));
    exact_cleanup(&calls);
}

#[tokio::test]
async fn init_and_exec_status_are_both_retained_before_seccomp_rejection() {
    for unfiltered_index in [2, 3] {
        let mut receipts = sequence(false);
        receipts[unfiltered_index]["stdout"] = json!("CapEff:\t0000000008000000\nSeccomp:\t0\n");
        receipts.drain(4..6);
        let (result, calls) = execute(receipts, false).await;
        let error = result
            .err()
            .unwrap_or_else(|| panic!("unfiltered process accepted"))
            .to_string();
        assert!(
            error.contains("default seccomp filter is not active"),
            "{error}"
        );
        assert!(
            error.contains("init_status") && error.contains("exec_status"),
            "{error}"
        );
        assert_eq!(calls[2].last().map(String::as_str), Some("/proc/1/status"));
        assert_eq!(
            calls[3].last().map(String::as_str),
            Some("/proc/self/status")
        );
        exact_cleanup(&calls);
    }
}

#[tokio::test]
async fn different_init_and_exec_capabilities_fail_with_both_receipts() {
    let mut receipts = sequence(false);
    receipts[3]["stdout"] = json!("CapEff:\t0000000008000001\nSeccomp:\t2\n");
    receipts.drain(4..6);
    let (result, calls) = execute(receipts, false).await;
    let error = result
        .err()
        .unwrap_or_else(|| panic!("capability mismatch accepted"))
        .to_string();
    assert!(error.contains("init/exec capability or seccomp observations differ"));
    assert!(error.contains("init_status") && error.contains("exec_status"));
    exact_cleanup(&calls);
}
