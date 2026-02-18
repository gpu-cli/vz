//! Linux VM backend for OCI containers on macOS.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

mod agent;
mod benchmark;
mod config;
mod error;
mod kernel;
mod vm;

pub use agent::{ExecOptions, OciExecOptions, open_port_forward_stream};
pub use benchmark::{
    BootBenchmarkConfig, BootBenchmarkEvent, BootBenchmarkResult, BootSample, run_boot_benchmark,
    run_boot_benchmark_with_progress,
};
pub use config::LinuxVmConfig;
pub use error::LinuxError;
pub use kernel::{
    EnsureKernelOptions, KernelPaths, KernelVersion, default_linux_dir, ensure_kernel,
    ensure_kernel_with_options,
};
pub use vm::LinuxVm;
pub use vz::protocol::{OciContainerState, OciExecResult};
