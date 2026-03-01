use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::grpc) fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(in crate::grpc) fn generate_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_receipt_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rcp-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_lease_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ls-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_container_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ctr-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_execution_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("exec-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_checkpoint_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ckpt-{}-{counter}", current_unix_secs())
}

pub(in crate::grpc) fn generate_fork_sandbox_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sbx-fork-{}-{counter}", current_unix_secs())
}
