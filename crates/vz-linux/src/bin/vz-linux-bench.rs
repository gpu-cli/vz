use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use clap::{ArgAction, Parser};
use tracing::info;
use vz_linux::{
    BootBenchmarkConfig, BootBenchmarkEvent, EnsureKernelOptions, LinuxVmConfig,
    ensure_kernel_with_options, run_boot_benchmark_with_progress,
};

#[derive(Debug, Parser)]
#[command(name = "vz-linux-bench")]
#[command(about = "Benchmark Linux VM cold boot to guest-agent-ready")]
struct Args {
    /// Number of benchmark iterations.
    #[arg(long, default_value_t = 5)]
    iterations: u32,

    /// Timeout in seconds waiting for guest agent readiness per run.
    #[arg(long, default_value_t = 5)]
    timeout_secs: u64,

    /// Path to kernel image (optional if using ensure_kernel bundle/install).
    #[arg(long)]
    kernel: Option<PathBuf>,

    /// Path to initramfs image (optional if using ensure_kernel bundle/install).
    #[arg(long)]
    initramfs: Option<PathBuf>,

    /// Optional predownloaded linux bundle directory.
    #[arg(long)]
    bundle_dir: Option<PathBuf>,

    /// Optional container rootfs directory to mount as VirtioFS `rootfs`.
    #[arg(long)]
    rootfs_dir: Option<PathBuf>,

    /// Optional install/cache directory (defaults to ~/.vz/linux).
    #[arg(long)]
    install_dir: Option<PathBuf>,

    /// Skip strict agent version check in version.json.
    #[arg(long, default_value_t = false)]
    no_version_check: bool,

    /// Show guest-side logs after each run (defaults to dmesg tail).
    #[arg(long, default_value_t = false)]
    guest_logs: bool,

    /// Guest shell command to capture logs (runs with `sh -lc`).
    #[arg(long)]
    guest_log_command: Option<String>,

    /// Optional URL for a curl-like guest HTTP smoke test via BusyBox wget.
    #[arg(long)]
    http_smoke_url: Option<String>,

    /// Timeout in seconds for the guest HTTP smoke request.
    #[arg(long, default_value_t = 5)]
    http_smoke_timeout_secs: u64,

    /// Timeout in seconds for guest log command execution.
    #[arg(long, default_value_t = 5)]
    guest_log_timeout_secs: u64,

    /// Print a waiting heartbeat every N readiness attempts.
    #[arg(long, default_value_t = 20)]
    retry_log_every: u32,

    /// Capture guest serial console output during boot.
    #[arg(long, action = ArgAction::Set, default_value_t = true)]
    serial_logs: bool,

    /// Optional directory for serial log files (default: system temp dir).
    #[arg(long)]
    serial_log_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .init();

    let args = Args::parse();

    info!("Resolving Linux kernel artifacts...");

    let (kernel, initramfs) = match (args.kernel, args.initramfs) {
        (Some(kernel), Some(initramfs)) => (kernel, initramfs),
        (None, None) => {
            let paths = ensure_kernel_with_options(EnsureKernelOptions {
                install_dir: args.install_dir,
                bundle_dir: args.bundle_dir,
                require_exact_agent_version: !args.no_version_check,
            })
            .await?;
            info!(
                "Using bundle artifacts: kernel={} initramfs={}",
                paths.kernel.display(),
                paths.initramfs.display()
            );
            (paths.kernel, paths.initramfs)
        }
        _ => {
            return Err(
                "provide both --kernel and --initramfs, or provide neither to use ensure_kernel"
                    .into(),
            );
        }
    };

    let guest_log_command = if let Some(command) = args.guest_log_command {
        Some(command)
    } else {
        let mut commands = Vec::new();
        if args.guest_logs {
            commands.push("dmesg | tail -n 120".to_string());
        }
        if let Some(url) = args.http_smoke_url.as_deref() {
            commands.push(build_http_smoke_command(url, args.http_smoke_timeout_secs));
        }

        if commands.is_empty() {
            None
        } else {
            Some(commands.join("\n"))
        }
    };

    let mut vm_config = LinuxVmConfig::new(kernel, initramfs);
    if let Some(rootfs_dir) = args.rootfs_dir {
        vm_config = vm_config.with_rootfs_dir(rootfs_dir);
    }
    info!(
        "Starting benchmark: iterations={} timeout={}s",
        args.iterations, args.timeout_secs
    );
    if let Some(command) = &guest_log_command {
        info!(
            "Guest log capture enabled: command='{}' timeout={}s",
            command, args.guest_log_timeout_secs
        );
    }
    if let Some(url) = args.http_smoke_url.as_deref() {
        info!(
            "Guest HTTP smoke enabled: url='{}' request_timeout={}s",
            url, args.http_smoke_timeout_secs
        );
    }
    if args.serial_logs {
        match &args.serial_log_dir {
            Some(dir) => info!("Serial log capture enabled: directory={}", dir.display()),
            None => info!("Serial log capture enabled: directory=<system temp>"),
        }
    }
    if let Some(rootfs_dir) = &vm_config.rootfs_dir {
        info!(
            "Rootfs VirtioFS enabled: directory={} tag=rootfs",
            rootfs_dir.display()
        );
    }

    let started = Instant::now();
    let result = run_boot_benchmark_with_progress(
        vm_config,
        BootBenchmarkConfig {
            iterations: args.iterations,
            agent_timeout: Duration::from_secs(args.timeout_secs),
            guest_log_command,
            guest_log_timeout: Duration::from_secs(args.guest_log_timeout_secs),
            retry_log_every: args.retry_log_every,
            capture_serial_logs: args.serial_logs,
            serial_log_dir: args.serial_log_dir,
        },
        |event| match event {
            BootBenchmarkEvent::IterationStarted { iteration, total } => {
                info!("[run {iteration}/{total}] creating VM");
            }
            BootBenchmarkEvent::VmCreated { iteration } => {
                info!("[run {iteration}] VM created, booting + waiting for guest agent...");
            }
            BootBenchmarkEvent::WaitingForAgent { iteration } => {
                info!("[run {iteration}] waiting for vsock handshake and ping...");
            }
            BootBenchmarkEvent::SerialLogPath { iteration, path } => {
                info!("[run {iteration}] serial log file: {}", path.display());
            }
            BootBenchmarkEvent::SerialLogOutput { iteration, output } => {
                print_prefixed_block(&format!("[run {iteration}] serial"), &output);
            }
            BootBenchmarkEvent::SerialLogReadError { iteration, error } => {
                info!("[run {iteration}] serial log read error: {error}");
            }
            BootBenchmarkEvent::AgentRetry {
                iteration,
                attempt,
                last_error,
            } => {
                info!("[run {iteration}] still waiting (attempt {attempt}): {last_error}");
            }
            BootBenchmarkEvent::AgentReady {
                iteration,
                boot_to_agent,
            } => {
                info!(
                    "[run {iteration}] guest agent ready in {:.3}s",
                    boot_to_agent.as_secs_f64()
                );
            }
            BootBenchmarkEvent::GuestLogStarted { iteration, command } => {
                info!("[run {iteration}] running guest log command: {command}");
            }
            BootBenchmarkEvent::GuestLogStdout { iteration, output } => {
                print_prefixed_block(&format!("[run {iteration}] guest stdout"), &output);
            }
            BootBenchmarkEvent::GuestLogStderr { iteration, output } => {
                print_prefixed_block(&format!("[run {iteration}] guest stderr"), &output);
            }
            BootBenchmarkEvent::GuestLogCompleted {
                iteration,
                exit_code,
            } => {
                info!("[run {iteration}] guest log command exit code: {exit_code}");
            }
            BootBenchmarkEvent::GuestLogFailed { iteration, error } => {
                info!("[run {iteration}] guest log command failed: {error}");
            }
            BootBenchmarkEvent::VmStopped { iteration } => {
                info!("[run {iteration}] VM stopped");
            }
        },
    )
    .await?;

    info!("Boot benchmark (cold start -> guest-agent-ready)");
    for sample in &result.samples {
        info!(
            "  run {:>2}: {:>6.3}s",
            sample.iteration,
            sample.boot_to_agent.as_secs_f64()
        );
    }

    info!("stats:");
    info!("  min:    {:>6.3}s", result.min.as_secs_f64());
    info!("  mean:   {:>6.3}s", result.mean.as_secs_f64());
    info!("  median: {:>6.3}s", result.median.as_secs_f64());
    info!("  p95:    {:>6.3}s", result.p95.as_secs_f64());
    info!("  max:    {:>6.3}s", result.max.as_secs_f64());
    info!("  total:  {:>6.3}s", started.elapsed().as_secs_f64());

    Ok(())
}

fn print_prefixed_block(prefix: &str, text: &str) {
    if text.trim().is_empty() {
        return;
    }

    info!("{prefix}:");
    for line in text.lines() {
        info!("  {}", strip_ansi_escape_sequences(line));
    }
}

fn build_http_smoke_command(url: &str, request_timeout_secs: u64) -> String {
    let quoted_url = shell_words::join([url]);
    format!(
        "if [ -x /bin/ip ] && [ -d /sys/class/net/eth0 ]; then /bin/ip link set dev eth0 up >/dev/null 2>&1 || true; fi\nif [ -x /sbin/udhcpc ] && [ -d /sys/class/net/eth0 ]; then /sbin/udhcpc -i eth0 -s /etc/udhcpc.script -q -n -t 3 -T 1 >/dev/null 2>&1 || true; fi\nif command -v wget >/dev/null 2>&1; then\n  wget -T {request_timeout_secs} -q -O /tmp/http-smoke.out {quoted_url}\nelif command -v curl >/dev/null 2>&1; then\n  curl -fsSL --max-time {request_timeout_secs} -o /tmp/http-smoke.out {quoted_url}\nelif [ -x /usr/local/bin/busybox ]; then\n  /usr/local/bin/busybox wget -T {request_timeout_secs} -q -O /tmp/http-smoke.out {quoted_url}\nelif [ -x /bin/busybox ]; then\n  /bin/busybox wget -T {request_timeout_secs} -q -O /tmp/http-smoke.out {quoted_url}\nelse\n  echo 'http smoke failed: no wget/curl available' >&2\n  exit 127\nfi\nif command -v head >/dev/null 2>&1; then\n  head -c 512 /tmp/http-smoke.out\nelif [ -x /usr/local/bin/busybox ]; then\n  /usr/local/bin/busybox head -c 512 /tmp/http-smoke.out\nelif [ -x /bin/busybox ]; then\n  /bin/busybox head -c 512 /tmp/http-smoke.out\nelse\n  cat /tmp/http-smoke.out\nfi"
    )
}

fn strip_ansi_escape_sequences(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            output.push(ch);
            continue;
        }

        if chars.next_if_eq(&'[').is_some() {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }

        output.push(ch);
    }

    output
}
