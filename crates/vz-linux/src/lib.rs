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
    EnsureKernelOptions, KernelBundle, KernelBundleOptions, KernelCapability, KernelFlavor,
    KernelPaths, KernelProfile, KernelVersion, default_linux_dir, default_linux_profile_dir,
    default_vz_linux_kernel_capabilities, default_vz_linux_kernel_profile_capabilities,
    ensure_kernel, ensure_kernel_bundle, ensure_kernel_profile, ensure_kernel_profile_with_options,
    ensure_kernel_with_options,
};
pub use vm::LinuxVm;
pub use vz::protocol::{NetworkServiceConfig, OciContainerState};
