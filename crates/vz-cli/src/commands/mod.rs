//! Topology lifecycle CLI adapters. Retired command modules are not compiled
//! into this binary; their backend APIs remain independently available.

pub mod dev_delete;
pub mod dev_exec;
pub mod dev_status;
pub mod dev_stop;
pub mod dev_up;
pub(crate) mod runtime_daemon;
