use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

use crate::{LinuxError, LinuxVm, LinuxVmConfig};

/// Configuration for boot benchmark runs.
#[derive(Debug, Clone)]
pub struct BootBenchmarkConfig {
    /// Number of cold-boot iterations.
    pub iterations: u32,
    /// Timeout waiting for guest agent readiness per iteration.
    pub agent_timeout: Duration,
    /// Optional shell command to run in the guest after readiness.
    pub guest_log_command: Option<String>,
    /// Timeout for guest log command execution.
    pub guest_log_timeout: Duration,
    /// Emit a retry event every N failed readiness attempts.
    pub retry_log_every: u32,
    /// Capture guest serial console output during boot.
    pub capture_serial_logs: bool,
    /// Optional directory where per-run serial logs are written.
    pub serial_log_dir: Option<PathBuf>,
}

impl Default for BootBenchmarkConfig {
    fn default() -> Self {
        Self {
            iterations: 5,
            agent_timeout: Duration::from_secs(5),
            guest_log_command: None,
            guest_log_timeout: Duration::from_secs(5),
            retry_log_every: 20,
            capture_serial_logs: true,
            serial_log_dir: None,
        }
    }
}

/// One benchmark sample.
#[derive(Debug, Clone, Copy)]
pub struct BootSample {
    /// 1-based iteration number.
    pub iteration: u32,
    /// Time from `start()` until guest-agent ping succeeds.
    pub boot_to_agent: Duration,
}

/// Progress events emitted during benchmark execution.
#[derive(Debug, Clone)]
pub enum BootBenchmarkEvent {
    /// Beginning a benchmark iteration.
    IterationStarted { iteration: u32, total: u32 },
    /// VM object was created successfully.
    VmCreated { iteration: u32 },
    /// VM start request was sent; now waiting for guest agent.
    WaitingForAgent { iteration: u32 },
    /// Per-run serial log file path.
    SerialLogPath { iteration: u32, path: PathBuf },
    /// New serial console output chunk.
    SerialLogOutput { iteration: u32, output: String },
    /// Failed to read serial log file.
    SerialLogReadError { iteration: u32, error: String },
    /// A readiness attempt failed; still waiting.
    AgentRetry {
        iteration: u32,
        attempt: u32,
        last_error: String,
    },
    /// Guest agent became reachable.
    AgentReady {
        iteration: u32,
        boot_to_agent: Duration,
    },
    /// Running optional guest log command.
    GuestLogStarted { iteration: u32, command: String },
    /// Guest log command stdout.
    GuestLogStdout { iteration: u32, output: String },
    /// Guest log command stderr.
    GuestLogStderr { iteration: u32, output: String },
    /// Guest log command completed.
    GuestLogCompleted { iteration: u32, exit_code: i32 },
    /// Guest log command failed.
    GuestLogFailed { iteration: u32, error: String },
    /// VM stop request completed.
    VmStopped { iteration: u32 },
}

/// Aggregated benchmark summary.
#[derive(Debug, Clone)]
pub struct BootBenchmarkResult {
    /// Per-iteration samples.
    pub samples: Vec<BootSample>,
    /// Minimum sample.
    pub min: Duration,
    /// Maximum sample.
    pub max: Duration,
    /// Arithmetic mean.
    pub mean: Duration,
    /// Median (p50).
    pub median: Duration,
    /// 95th percentile.
    pub p95: Duration,
}

/// Run a cold-boot benchmark for Linux VM startup.
pub async fn run_boot_benchmark(
    vm_config: LinuxVmConfig,
    config: BootBenchmarkConfig,
) -> Result<BootBenchmarkResult, LinuxError> {
    run_boot_benchmark_with_progress(vm_config, config, |_| {}).await
}

/// Run a cold-boot benchmark and emit progress callbacks.
pub async fn run_boot_benchmark_with_progress<F>(
    vm_config: LinuxVmConfig,
    config: BootBenchmarkConfig,
    mut on_event: F,
) -> Result<BootBenchmarkResult, LinuxError>
where
    F: FnMut(BootBenchmarkEvent),
{
    if config.iterations == 0 {
        return Err(LinuxError::InvalidConfig(
            "benchmark iterations must be greater than 0".to_string(),
        ));
    }

    let mut samples = Vec::with_capacity(config.iterations as usize);

    for iteration in 1..=config.iterations {
        on_event(BootBenchmarkEvent::IterationStarted {
            iteration,
            total: config.iterations,
        });

        let mut iteration_config = vm_config.clone();
        let mut serial_offset = 0usize;
        let serial_log_path = if config.capture_serial_logs {
            let path = serial_log_path(iteration, config.serial_log_dir.as_deref());
            iteration_config.serial_log_file = Some(path.clone());
            on_event(BootBenchmarkEvent::SerialLogPath {
                iteration,
                path: path.clone(),
            });
            Some(path)
        } else {
            None
        };

        let vm = LinuxVm::create(iteration_config).await?;
        on_event(BootBenchmarkEvent::VmCreated { iteration });

        let boot_started = Instant::now();
        vm.start().await?;

        on_event(BootBenchmarkEvent::WaitingForAgent { iteration });

        match vm
            .wait_for_agent_with_progress(config.agent_timeout, |attempt, last_error| {
                if config.retry_log_every > 0 && attempt % config.retry_log_every == 0 {
                    on_event(BootBenchmarkEvent::AgentRetry {
                        iteration,
                        attempt,
                        last_error: last_error.to_string(),
                    });
                }

                if let Some(path) = serial_log_path.as_deref() {
                    emit_serial_log_chunk(path, &mut serial_offset, iteration, &mut on_event);
                }
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                if let Some(path) = serial_log_path.as_deref() {
                    emit_serial_log_chunk(path, &mut serial_offset, iteration, &mut on_event);
                }
                let _ = vm.stop().await;
                return Err(e);
            }
        }
        let boot_to_agent = boot_started.elapsed();

        on_event(BootBenchmarkEvent::AgentReady {
            iteration,
            boot_to_agent,
        });

        if let Some(path) = serial_log_path.as_deref() {
            emit_serial_log_chunk(path, &mut serial_offset, iteration, &mut on_event);
        }

        if let Some(command) = &config.guest_log_command {
            on_event(BootBenchmarkEvent::GuestLogStarted {
                iteration,
                command: command.clone(),
            });

            match vm
                .exec_collect(
                    "sh".to_string(),
                    vec!["-lc".to_string(), command.clone()],
                    config.guest_log_timeout,
                )
                .await
            {
                Ok(output) => {
                    if !output.stdout.is_empty() {
                        on_event(BootBenchmarkEvent::GuestLogStdout {
                            iteration,
                            output: output.stdout,
                        });
                    }
                    if !output.stderr.is_empty() {
                        on_event(BootBenchmarkEvent::GuestLogStderr {
                            iteration,
                            output: output.stderr,
                        });
                    }
                    on_event(BootBenchmarkEvent::GuestLogCompleted {
                        iteration,
                        exit_code: output.exit_code,
                    });
                }
                Err(e) => {
                    on_event(BootBenchmarkEvent::GuestLogFailed {
                        iteration,
                        error: e.to_string(),
                    });
                }
            }
        }

        vm.stop().await?;
        on_event(BootBenchmarkEvent::VmStopped { iteration });

        if let Some(path) = serial_log_path.as_deref() {
            emit_serial_log_chunk(path, &mut serial_offset, iteration, &mut on_event);
        }

        samples.push(BootSample {
            iteration,
            boot_to_agent,
        });
    }

    Ok(summarize(samples))
}

fn serial_log_path(iteration: u32, log_dir: Option<&Path>) -> PathBuf {
    let dir = match log_dir {
        Some(path) => path.to_path_buf(),
        None => std::env::temp_dir(),
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    dir.join(format!(
        "vz-linux-bench-{}-run{iteration}-{timestamp}.serial.log",
        std::process::id()
    ))
}

fn emit_serial_log_chunk<F>(path: &Path, offset: &mut usize, iteration: u32, on_event: &mut F)
where
    F: FnMut(BootBenchmarkEvent),
{
    match read_serial_log_chunk(path, offset) {
        Ok(Some(output)) if !output.is_empty() => {
            on_event(BootBenchmarkEvent::SerialLogOutput { iteration, output });
        }
        Ok(_) => {}
        Err(error) => {
            on_event(BootBenchmarkEvent::SerialLogReadError {
                iteration,
                error: format!("{}: {error}", path.display()),
            });
        }
    }
}

fn read_serial_log_chunk(
    path: &Path,
    offset: &mut usize,
) -> Result<Option<String>, std::io::Error> {
    if !path.exists() {
        return Ok(None);
    }

    let bytes = std::fs::read(path)?;
    if bytes.len() <= *offset {
        return Ok(None);
    }

    let start = *offset;
    *offset = bytes.len();

    Ok(Some(String::from_utf8_lossy(&bytes[start..]).into_owned()))
}

fn summarize(samples: Vec<BootSample>) -> BootBenchmarkResult {
    let mut sorted_nanos: Vec<u128> = samples.iter().map(|s| s.boot_to_agent.as_nanos()).collect();
    sorted_nanos.sort_unstable();

    let len = sorted_nanos.len();
    let min = duration_from_nanos(sorted_nanos[0]);
    let max = duration_from_nanos(sorted_nanos[len - 1]);
    let mean = duration_from_nanos(sorted_nanos.iter().copied().sum::<u128>() / len as u128);

    let median_nanos = if len % 2 == 1 {
        sorted_nanos[len / 2]
    } else {
        let a = sorted_nanos[(len / 2) - 1];
        let b = sorted_nanos[len / 2];
        (a + b) / 2
    };
    let median = duration_from_nanos(median_nanos);

    let p95_index = len.saturating_mul(95).div_ceil(100).saturating_sub(1);
    let p95 = duration_from_nanos(sorted_nanos[p95_index]);

    BootBenchmarkResult {
        samples,
        min,
        max,
        mean,
        median,
        p95,
    }
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let capped = nanos.min(u128::from(u64::MAX));
    Duration::from_nanos(capped as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(iter: u32, ms: u64) -> BootSample {
        BootSample {
            iteration: iter,
            boot_to_agent: Duration::from_millis(ms),
        }
    }

    #[test]
    fn summarize_odd_number_of_samples() {
        let result = summarize(vec![
            sample(1, 100),
            sample(2, 500),
            sample(3, 300),
            sample(4, 200),
            sample(5, 400),
        ]);

        assert_eq!(result.min, Duration::from_millis(100));
        assert_eq!(result.max, Duration::from_millis(500));
        assert_eq!(result.mean, Duration::from_millis(300));
        assert_eq!(result.median, Duration::from_millis(300));
        assert_eq!(result.p95, Duration::from_millis(500));
    }

    #[test]
    fn summarize_even_number_of_samples() {
        let result = summarize(vec![
            sample(1, 100),
            sample(2, 200),
            sample(3, 300),
            sample(4, 400),
        ]);

        assert_eq!(result.median, Duration::from_millis(250));
        assert_eq!(result.p95, Duration::from_millis(400));
    }
}
