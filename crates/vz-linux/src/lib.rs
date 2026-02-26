//! Linux VM backend for OCI containers on macOS.

#![cfg(target_os = "macos")]
#![forbid(unsafe_code)]

mod benchmark;
mod config;
mod error;
pub mod grpc_client;
mod kernel;
mod vm;

pub use benchmark::{
    BootBenchmarkConfig, BootBenchmarkEvent, BootBenchmarkResult, BootSample, run_boot_benchmark,
    run_boot_benchmark_with_progress,
};
pub use config::LinuxVmConfig;
pub use error::LinuxError;
pub use grpc_client::{ExecOptions, GrpcExecStream, GrpcPortForwardStream, OciExecOptions};
pub use kernel::{
    EnsureKernelOptions, KernelPaths, KernelVersion, default_linux_dir, ensure_kernel,
    ensure_kernel_with_options,
};
pub use vm::LinuxVm;
pub use vz::protocol::{NetworkServiceConfig, OciContainerState, OciExecResult};
