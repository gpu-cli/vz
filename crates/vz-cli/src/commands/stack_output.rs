//! Unified user-facing output for `vz stack up` and `vz stack down`.
//!
//! [`StackOutput`] owns all user-visible output during stack orchestration.
//! It detects TTY at construction and renders via `indicatif::MultiProgress`
//! (interactive spinners) or plain `println!` (CI / piped output).
//!
//! No other code in the stack command path should call `println!` directly
//! for orchestration progress — use [`StackOutput::message`] instead.

// Progress bar template strings are static and known-valid; allow expect on them.
#![allow(clippy::expect_used)]

use std::collections::HashSet;
use std::io::IsTerminal;
use std::time::{Duration, Instant};

use console::style;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use vz_stack::{
    ApplyResult, ExecutionResult, HealthPollResult, OrchestrationResult, RoundReport, StackEvent,
};

/// Per-service display phase.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Pending,
    Deferred,
    Creating,
    Running,
    Ready,
    Failed,
    Removing,
    Removed,
}

/// Per-service progress bar and phase tracking.
struct ServiceBar {
    bar: ProgressBar,
    phase: Phase,
    started_at: Instant,
    /// Spinner style for active phases (Creating, Running, Removing).
    spinner_style: ProgressStyle,
}

/// Unified output for stack orchestration commands.
///
/// In TTY mode, renders `indicatif` progress bars with spinners.
/// In non-TTY mode, emits clean `println!` lines on phase transitions.
pub struct StackOutput {
    multi: MultiProgress,
    header: ProgressBar,
    /// Ordered (service_name, bar) for stable display.
    services: Vec<(String, ServiceBar)>,
    /// Services that have a health check configured.
    has_health: HashSet<String>,
    start: Instant,
    is_tty: bool,
    total: usize,
    done_count: usize,
    fail_count: usize,
    round: usize,
    last_activity: Instant,
    last_plain_heartbeat: Instant,
}

impl StackOutput {
    /// Create output for `vz stack up`.
    pub fn new(spec: &vz_stack::StackSpec) -> Self {
        let is_tty = std::io::stderr().is_terminal();
        let total = spec.services.len();

        let multi = MultiProgress::new();
        if !is_tty {
            multi.set_draw_target(ProgressDrawTarget::hidden());
        }

        // Header bar — shows overall progress.
        let header = multi.add(ProgressBar::new(total as u64));
        header.set_style(
            ProgressStyle::default_bar()
                .template("{msg}")
                .expect("valid template"),
        );
        header.set_message(format!(
            "{} Running 0/{}",
            style("[+]").bold().cyan(),
            total
        ));

        // Build health check lookup.
        let has_health: HashSet<String> = spec
            .services
            .iter()
            .filter(|s| s.healthcheck.is_some())
            .map(|s| s.name.clone())
            .collect();

        // One spinner bar per service.
        let spinner_style = ProgressStyle::default_spinner()
            .template("   {spinner:.cyan} {msg}")
            .expect("valid template");
        let pending_style = ProgressStyle::default_spinner()
            .template("   {msg}")
            .expect("valid template");

        let now = Instant::now();
        let services: Vec<(String, ServiceBar)> = spec
            .services
            .iter()
            .map(|s| {
                let bar = multi.add(ProgressBar::new_spinner());
                bar.set_style(pending_style.clone());
                bar.set_message(format!(
                    "{} {:<20} {}",
                    style("·").dim(),
                    s.name,
                    style("Pending").dim()
                ));
                bar.enable_steady_tick(std::time::Duration::from_millis(100));
                (
                    s.name.clone(),
                    ServiceBar {
                        bar,
                        phase: Phase::Pending,
                        started_at: now,
                        spinner_style: spinner_style.clone(),
                    },
                )
            })
            .collect();

        Self {
            multi,
            header,
            services,
            has_health,
            start: now,
            is_tty,
            total,
            done_count: 0,
            fail_count: 0,
            round: 0,
            last_activity: now,
            last_plain_heartbeat: now,
        }
    }

    /// Process an orchestration round report — updates all service bars.
    pub fn on_round(&mut self, report: &RoundReport) {
        self.round = report.round;
        self.last_activity = Instant::now();
        // 1. Apply result → transition to Creating/Removing
        self.process_apply(&report.apply_result);

        // 2. Execution result → transition to Running/Ready/Failed
        if let Some(ref exec) = report.exec_result {
            self.process_exec(&report.apply_result, exec);
        }

        // 3. Health result → transition newly_ready/newly_failed
        if let Some(ref health) = report.health_result {
            self.process_health(health);
        }

        // Update header.
        self.update_header();
    }

    /// Process a real-time stack event while orchestration is running.
    pub fn on_event(&mut self, event: &StackEvent) {
        self.last_activity = Instant::now();
        match event {
            StackEvent::ServiceCreating { service_name, .. } => self.mark_creating(service_name),
            StackEvent::ServiceReady { service_name, .. } => self.mark_ready(service_name),
            StackEvent::ServiceStopping { service_name, .. } => self.mark_removing(service_name),
            StackEvent::ServiceStopped { service_name, .. } => self.mark_removed(service_name),
            StackEvent::ServiceFailed {
                service_name,
                error,
                ..
            } => self.mark_failed(service_name, error),
            StackEvent::HealthCheckPassed { service_name, .. } => self.mark_ready(service_name),
            StackEvent::HealthCheckFailed {
                service_name,
                error,
                ..
            } => self.mark_failed(service_name, error),
            StackEvent::DependencyBlocked {
                service_name,
                waiting_on,
                ..
            } => self.mark_deferred(service_name, waiting_on),
            StackEvent::MountTopologyRecreateRequired {
                service_name,
                previous_digest,
                desired_digest,
                ..
            } => {
                self.message(&format!(
                    " {} {} mount topology changed ({:?} -> {desired_digest}); recreate required",
                    style("!").bold().yellow(),
                    service_name,
                    previous_digest.as_deref().unwrap_or("<none>")
                ));
            }
            StackEvent::PortConflict {
                service_name, port, ..
            } => {
                self.message(&format!(
                    " {} {} requested host port {} already in use",
                    style("!").bold().red(),
                    service_name,
                    port
                ));
            }
            StackEvent::VolumeCreated { volume_name, .. } => {
                self.message(&format!(
                    " {} volume {} ready",
                    style("+").bold().cyan(),
                    volume_name
                ));
            }
            StackEvent::StackApplyFailed { error, .. } => {
                self.message(&format!(" {} {}", style("!").bold().red(), error));
            }
            StackEvent::StackApplyStarted { .. }
            | StackEvent::StackApplyCompleted { .. }
            | StackEvent::StackDestroyed { .. } => {}
            StackEvent::SandboxCreating { sandbox_id, .. } => {
                self.message(&format!(
                    " {} sandbox {} creating",
                    style("+").bold().cyan(),
                    sandbox_id
                ));
            }
            StackEvent::SandboxReady { sandbox_id, .. } => {
                self.message(&format!(
                    " {} sandbox {} ready",
                    style("+").bold().green(),
                    sandbox_id
                ));
            }
            StackEvent::SandboxDraining { sandbox_id, .. } => {
                self.message(&format!(
                    " {} sandbox {} draining",
                    style("-").bold().yellow(),
                    sandbox_id
                ));
            }
            StackEvent::SandboxTerminated { sandbox_id, .. } => {
                self.message(&format!(
                    " {} sandbox {} terminated",
                    style("-").bold().dim(),
                    sandbox_id
                ));
            }
            StackEvent::SandboxFailed {
                sandbox_id, error, ..
            } => {
                self.message(&format!(
                    " {} sandbox {} failed: {error}",
                    style("!").bold().red(),
                    sandbox_id
                ));
            }
            StackEvent::LeaseOpened { lease_id, .. } => {
                self.message(&format!(
                    " {} lease {} opened",
                    style("+").bold().cyan(),
                    lease_id
                ));
            }
            StackEvent::LeaseHeartbeat { lease_id } => {
                self.message(&format!(
                    " {} lease {} heartbeat",
                    style(".").bold().blue(),
                    lease_id
                ));
            }
            StackEvent::LeaseExpired { lease_id } => {
                self.message(&format!(
                    " {} lease {} expired",
                    style("!").bold().yellow(),
                    lease_id
                ));
            }
            StackEvent::LeaseClosed { lease_id } => {
                self.message(&format!(
                    " {} lease {} closed",
                    style("-").bold().dim(),
                    lease_id
                ));
            }
            StackEvent::LeaseFailed { lease_id, error } => {
                self.message(&format!(
                    " {} lease {} failed: {error}",
                    style("!").bold().red(),
                    lease_id
                ));
            }
            StackEvent::BuildQueued { .. }
            | StackEvent::BuildRunning { .. }
            | StackEvent::BuildSucceeded { .. }
            | StackEvent::BuildFailed { .. }
            | StackEvent::BuildCanceled { .. }
            | StackEvent::ExecutionQueued { .. }
            | StackEvent::ExecutionRunning { .. }
            | StackEvent::ExecutionExited { .. }
            | StackEvent::ExecutionFailed { .. }
            | StackEvent::ExecutionCanceled { .. }
            | StackEvent::ExecutionResized { .. }
            | StackEvent::ExecutionSignaled { .. }
            | StackEvent::CheckpointCreating { .. }
            | StackEvent::CheckpointReady { .. }
            | StackEvent::CheckpointFailed { .. }
            | StackEvent::CheckpointRestored { .. }
            | StackEvent::CheckpointForked { .. }
            | StackEvent::ContainerCreated { .. }
            | StackEvent::ContainerStarting { .. }
            | StackEvent::ContainerRunning { .. }
            | StackEvent::ContainerStopping { .. }
            | StackEvent::ContainerExited { .. }
            | StackEvent::ContainerFailed { .. }
            | StackEvent::ContainerRemoved { .. }
            | StackEvent::DriftDetected { .. } => {}
        }
        self.update_header();
    }

    /// Periodic tick to keep progress visibly active between events.
    pub fn tick(&mut self) {
        self.update_header();
        if !self.is_tty && self.last_plain_heartbeat.elapsed() >= Duration::from_secs(4) {
            let active = self
                .total
                .saturating_sub(self.done_count)
                .saturating_sub(self.fail_count);
            println!(
                "stack progress: ready={}, active={}, failed={}, round={}, elapsed={:.1}s",
                self.done_count,
                active,
                self.fail_count,
                self.round.max(1),
                self.start.elapsed().as_secs_f64()
            );
            self.last_plain_heartbeat = Instant::now();
        }
    }

    /// Finalize output after orchestration completes.
    pub fn finish(&self, result: &OrchestrationResult) {
        let elapsed = format!("{:.1}s", self.start.elapsed().as_secs_f64());

        if result.converged && result.services_failed == 0 {
            let msg = format!(
                "{} Stack ready \u{2014} {} services ({})",
                style("[\u{2714}]").bold().green(),
                result.services_ready,
                elapsed,
            );
            if self.is_tty {
                self.header.set_message(msg);
                self.header.finish();
            } else {
                println!("{msg}");
            }
        } else if result.services_failed > 0 {
            let msg = format!(
                "{} Stack failed \u{2014} {} ready, {} failed ({})",
                style("[\u{2718}]").bold().red(),
                result.services_ready,
                result.services_failed,
                elapsed,
            );
            if self.is_tty {
                self.header.set_message(msg);
                self.header.finish();
            } else {
                println!("{msg}");
            }
        } else {
            let msg = format!(
                "{} Stack did not converge \u{2014} {} ready, {} pending ({} rounds, {})",
                style("[!]").bold().yellow(),
                result.services_ready,
                self.total
                    .saturating_sub(result.services_ready)
                    .saturating_sub(result.services_failed),
                result.rounds,
                elapsed,
            );
            if self.is_tty {
                self.header.set_message(msg);
                self.header.finish();
            } else {
                println!("{msg}");
            }
        }
    }

    /// Create output for `vz stack down`.
    pub fn new_down(service_names: &[String]) -> Self {
        let is_tty = std::io::stderr().is_terminal();
        let total = service_names.len();

        let multi = MultiProgress::new();
        if !is_tty {
            multi.set_draw_target(ProgressDrawTarget::hidden());
        }

        let header = multi.add(ProgressBar::new(total as u64));
        header.set_style(
            ProgressStyle::default_bar()
                .template("{msg}")
                .expect("valid template"),
        );
        header.set_message(format!(
            "{} Stopping 0/{}",
            style("[-]").bold().cyan(),
            total
        ));

        let spinner_style = ProgressStyle::default_spinner()
            .template("   {spinner:.cyan} {msg}")
            .expect("valid template");

        let now = Instant::now();
        let services: Vec<(String, ServiceBar)> = service_names
            .iter()
            .map(|name| {
                let bar = multi.add(ProgressBar::new_spinner());
                bar.set_style(spinner_style.clone());
                bar.set_message(format!("{:<20} {}", name, style("Removing...").cyan()));
                bar.enable_steady_tick(std::time::Duration::from_millis(100));
                (
                    name.clone(),
                    ServiceBar {
                        bar,
                        phase: Phase::Removing,
                        started_at: now,
                        spinner_style: spinner_style.clone(),
                    },
                )
            })
            .collect();

        Self {
            multi,
            header,
            services,
            has_health: HashSet::new(),
            start: now,
            is_tty,
            total,
            done_count: 0,
            fail_count: 0,
            round: 0,
            last_activity: now,
            last_plain_heartbeat: now,
        }
    }

    /// Process teardown results.
    pub fn on_down(&mut self, _apply: &ApplyResult, exec: &ExecutionResult) {
        if exec.all_succeeded() {
            for (name, svc) in &mut self.services {
                if svc.phase == Phase::Removing {
                    let elapsed = format!("{:.1}s", svc.started_at.elapsed().as_secs_f64());
                    svc.phase = Phase::Removed;
                    self.done_count += 1;

                    let finished_style = ProgressStyle::default_spinner()
                        .template("   {msg}")
                        .expect("valid template");
                    svc.bar.set_style(finished_style);
                    svc.bar.set_message(format!(
                        "{} {:<20} {}",
                        style("\u{2714}").green(),
                        name,
                        style(format!("Removed  {elapsed}")).green()
                    ));
                    svc.bar.finish();

                    if !self.is_tty {
                        println!("{}: Removed ({elapsed})", name);
                    }
                }
            }
        } else {
            // Mark succeeded services as removed, failed as failed.
            let failed_names: HashSet<&str> = exec.errors.iter().map(|(n, _)| n.as_str()).collect();

            for (name, svc) in &mut self.services {
                if svc.phase != Phase::Removing {
                    continue;
                }
                let elapsed = format!("{:.1}s", svc.started_at.elapsed().as_secs_f64());
                let finished_style = ProgressStyle::default_spinner()
                    .template("   {msg}")
                    .expect("valid template");
                svc.bar.set_style(finished_style);

                if failed_names.contains(name.as_str()) {
                    let error_msg = exec
                        .errors
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, e)| e.as_str())
                        .unwrap_or("unknown error");
                    svc.phase = Phase::Failed;
                    self.fail_count += 1;
                    svc.bar.set_message(format!(
                        "{} {:<20} {}",
                        style("\u{2718}").red(),
                        name,
                        style(format!("Failed: {error_msg}")).red()
                    ));
                    svc.bar.finish();

                    if !self.is_tty {
                        println!("{}: Failed: {error_msg}", name);
                    }
                } else {
                    svc.phase = Phase::Removed;
                    self.done_count += 1;
                    svc.bar.set_message(format!(
                        "{} {:<20} {}",
                        style("\u{2714}").green(),
                        name,
                        style(format!("Removed  {elapsed}")).green()
                    ));
                    svc.bar.finish();

                    if !self.is_tty {
                        println!("{}: Removed ({elapsed})", name);
                    }
                }
            }
        }

        self.update_header_down();
    }

    /// Finalize teardown output.
    pub fn finish_down(&self) {
        let elapsed = format!("{:.1}s", self.start.elapsed().as_secs_f64());

        if self.fail_count == 0 {
            let msg = format!(
                "{} Stopped {} services ({})",
                style("[\u{2714}]").bold().green(),
                self.done_count,
                elapsed,
            );
            if self.is_tty {
                self.header.set_message(msg);
                self.header.finish();
            } else {
                println!("{msg}");
            }
        } else {
            let msg = format!(
                "{} Stopped with errors \u{2014} {} removed, {} failed ({})",
                style("[\u{2718}]").bold().red(),
                self.done_count,
                self.fail_count,
                elapsed,
            );
            if self.is_tty {
                self.header.set_message(msg);
                self.header.finish();
            } else {
                println!("{msg}");
            }
        }
    }

    /// Render skipped mount warnings below the header.
    ///
    /// Called after the first round report to surface bind mounts that
    /// were silently removed during validation (dangling symlinks, sockets, etc.).
    pub fn on_warnings(&self, skipped: &[vz_stack::SkippedMount]) {
        for skip in skipped {
            let msg = format!(
                " {} skipped mount {} \u{2192} {} ({})",
                style("\u{26a0}").yellow(),
                style(&skip.source).dim(),
                style(&skip.target).dim(),
                skip.reason,
            );
            self.message(&msg);
        }
    }

    /// Print a general-purpose message that doesn't clobber progress bars.
    pub fn message(&self, msg: &str) {
        if self.is_tty {
            let _ = self.multi.println(msg);
        } else {
            println!("{msg}");
        }
    }

    // ── internal helpers ────────────────────────────────────────────

    fn service_index(&self, name: &str) -> Option<usize> {
        self.services
            .iter()
            .position(|(service, _)| service == name)
    }

    fn mark_creating(&mut self, name: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let svc = &mut self.services[index].1;
        if svc.phase != Phase::Pending && svc.phase != Phase::Deferred {
            return;
        }

        svc.phase = Phase::Creating;
        svc.started_at = Instant::now();
        svc.bar.set_style(svc.spinner_style.clone());
        svc.bar
            .set_message(format!("{:<20} {}", name, style("Creating...").cyan()));

        if !self.is_tty {
            println!("{}: Creating...", name);
        }
    }

    fn mark_deferred(&mut self, name: &str, waiting_on: &[String]) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let svc = &mut self.services[index].1;
        if svc.phase != Phase::Pending && svc.phase != Phase::Deferred {
            return;
        }

        svc.phase = Phase::Deferred;
        let deps = waiting_on.join(", ");
        let pending_style = ProgressStyle::default_spinner()
            .template("   {msg}")
            .expect("valid template");
        svc.bar.set_style(pending_style);
        svc.bar.set_message(format!(
            "{} {:<20} {}",
            style("\u{00b7}").yellow(),
            name,
            style(format!("Waiting ({deps})")).yellow()
        ));
    }

    fn mark_health_checking(&mut self, name: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let svc = &mut self.services[index].1;
        if svc.phase == Phase::Failed || svc.phase == Phase::Ready {
            return;
        }

        svc.phase = Phase::Running;
        svc.bar.set_style(svc.spinner_style.clone());
        svc.bar.set_message(format!(
            "{:<20} {}",
            name,
            style("Health check...").yellow()
        ));
    }

    fn mark_ready(&mut self, name: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let previous = self.services[index].1.phase;
        if previous == Phase::Ready {
            return;
        }

        if previous == Phase::Failed && self.fail_count > 0 {
            self.fail_count -= 1;
        }
        if previous != Phase::Removed {
            self.done_count = self.done_count.saturating_add(1);
        }

        let svc = &mut self.services[index].1;
        set_ready(svc, name, self.is_tty);
    }

    fn mark_failed(&mut self, name: &str, error: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let previous = self.services[index].1.phase;
        if previous == Phase::Failed {
            return;
        }

        if matches!(previous, Phase::Ready | Phase::Removed) && self.done_count > 0 {
            self.done_count -= 1;
        }
        self.fail_count = self.fail_count.saturating_add(1);

        let svc = &mut self.services[index].1;
        set_failed(svc, name, error, self.is_tty);
    }

    fn mark_removing(&mut self, name: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let svc = &mut self.services[index].1;
        if svc.phase == Phase::Removing || svc.phase == Phase::Removed {
            return;
        }

        svc.phase = Phase::Removing;
        svc.started_at = Instant::now();
        svc.bar.set_style(svc.spinner_style.clone());
        svc.bar
            .set_message(format!("{:<20} {}", name, style("Removing...").cyan()));

        if !self.is_tty {
            println!("{}: Removing...", name);
        }
    }

    fn mark_removed(&mut self, name: &str) {
        let Some(index) = self.service_index(name) else {
            return;
        };
        let previous = self.services[index].1.phase;
        if previous == Phase::Removed {
            return;
        }

        if previous == Phase::Failed && self.fail_count > 0 {
            self.fail_count -= 1;
        }
        self.done_count = self.done_count.saturating_add(1);

        let svc = &mut self.services[index].1;
        set_removed(svc, name, self.is_tty);
    }

    fn process_apply(&mut self, apply: &ApplyResult) {
        for action in &apply.actions {
            let name = action.service_name();
            match action {
                vz_stack::Action::ServiceCreate { .. }
                | vz_stack::Action::ServiceRecreate { .. } => {
                    self.mark_creating(name);
                }
                vz_stack::Action::ServiceRemove { .. } => {
                    self.mark_removing(name);
                }
            }
        }

        // Mark deferred services.
        for deferred in &apply.deferred {
            self.mark_deferred(&deferred.service_name, &deferred.waiting_on);
        }
    }

    fn process_exec(&mut self, apply: &ApplyResult, exec: &ExecutionResult) {
        let failed_names: HashSet<&str> = exec.errors.iter().map(|(n, _)| n.as_str()).collect();

        for action in &apply.actions {
            let name = action.service_name();
            match action {
                vz_stack::Action::ServiceCreate { .. }
                | vz_stack::Action::ServiceRecreate { .. } => {
                    if failed_names.contains(name) {
                        let error_msg = exec
                            .errors
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, e)| e.as_str())
                            .unwrap_or("unknown error");
                        self.mark_failed(name, error_msg);
                    } else if self.has_health.contains(name) {
                        self.mark_health_checking(name);
                    } else {
                        self.mark_ready(name);
                    }
                }
                vz_stack::Action::ServiceRemove { .. } => {
                    if failed_names.contains(name) {
                        let error_msg = exec
                            .errors
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, e)| e.as_str())
                            .unwrap_or("unknown error");
                        self.mark_failed(name, error_msg);
                    } else {
                        self.mark_removed(name);
                    }
                }
            }
        }
    }

    fn process_health(&mut self, health: &HealthPollResult) {
        for name in &health.newly_ready {
            self.mark_ready(name);
        }

        for name in &health.newly_failed {
            self.mark_failed(name, "health check failed");
        }
    }

    fn update_header(&self) {
        let active = self
            .total
            .saturating_sub(self.done_count)
            .saturating_sub(self.fail_count);
        let elapsed = self.start.elapsed().as_secs_f64();
        let idle = self.last_activity.elapsed().as_secs_f64();
        let msg = format!(
            "{} Running {}/{} | active {} | failed {} | round {} | {:.1}s elapsed | {:.1}s idle",
            style("[+]").bold().cyan(),
            self.done_count,
            self.total,
            active,
            self.fail_count,
            self.round.max(1),
            elapsed,
            idle,
        );
        self.header.set_message(msg);
    }

    fn update_header_down(&self) {
        let active = self
            .total
            .saturating_sub(self.done_count)
            .saturating_sub(self.fail_count);
        let elapsed = self.start.elapsed().as_secs_f64();
        let msg = format!(
            "{} Stopping {}/{} | active {} | failed {} | {:.1}s elapsed",
            style("[-]").bold().cyan(),
            self.done_count,
            self.total,
            active,
            self.fail_count,
            elapsed,
        );
        self.header.set_message(msg);
    }
}

// ── Free-standing transition helpers (avoid borrow conflicts) ──────

/// Finished-bar style (no spinner, just a message).
fn finished_style() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("   {msg}")
        .expect("valid template")
}

fn set_ready(svc: &mut ServiceBar, name: &str, is_tty: bool) {
    let elapsed = format!("{:.1}s", svc.started_at.elapsed().as_secs_f64());
    svc.phase = Phase::Ready;
    svc.bar.set_style(finished_style());
    svc.bar.set_message(format!(
        "{} {:<20} {}",
        style("\u{2714}").green(),
        name,
        style(format!("Started  {elapsed}")).green()
    ));
    svc.bar.finish();
    if !is_tty {
        println!("{}: Started ({elapsed})", name);
    }
}

fn set_failed(svc: &mut ServiceBar, name: &str, error: &str, is_tty: bool) {
    let display_error = truncate_error(error);
    svc.phase = Phase::Failed;
    svc.bar.set_style(finished_style());
    svc.bar.set_message(format!(
        "{} {:<20} {}",
        style("\u{2718}").red(),
        name,
        style(format!("Failed: {display_error}")).red()
    ));
    svc.bar.finish();
    if !is_tty {
        println!("{}: Failed: {display_error}", name);
    }
}

/// Truncate an error message for user-facing display.
///
/// - Extracts the leaf error from colon-separated error chains.
/// - Truncates sha256 digests to 7 characters.
/// - Caps total length at 100 characters.
fn truncate_error(msg: &str) -> String {
    // Extract the last meaningful segment from error chains like
    // "network error: create_in_sandbox failed: storage operation failed: unable to unpack layer..."
    let leaf = msg.rsplit(": ").next().unwrap_or(msg).trim();

    // Truncate sha256 digests: sha256:e54bc7400b8c... → sha256:e54bc74…
    let mut result = String::with_capacity(leaf.len());
    let mut rest = leaf;
    while let Some(idx) = rest.find("sha256:") {
        result.push_str(&rest[..idx]);
        let digest_start = idx + 7; // len("sha256:")
        let after_prefix = &rest[digest_start..];
        let hex_len = after_prefix
            .chars()
            .take_while(|c| c.is_ascii_hexdigit())
            .count();
        if hex_len > 7 {
            result.push_str("sha256:");
            result.push_str(&after_prefix[..7]);
            result.push('\u{2026}');
            rest = &rest[digest_start + hex_len..];
        } else {
            result.push_str(&rest[idx..digest_start]);
            rest = after_prefix;
        }
    }
    result.push_str(rest);

    if result.len() > 100 {
        let truncated: String = result.chars().take(97).collect();
        format!("{truncated}...")
    } else {
        result
    }
}

fn set_removed(svc: &mut ServiceBar, name: &str, is_tty: bool) {
    let elapsed = format!("{:.1}s", svc.started_at.elapsed().as_secs_f64());
    svc.phase = Phase::Removed;
    svc.bar.set_style(finished_style());
    svc.bar.set_message(format!(
        "{} {:<20} {}",
        style("\u{2714}").green(),
        name,
        style(format!("Removed  {elapsed}")).green()
    ));
    svc.bar.finish();
    if !is_tty {
        println!("{}: Removed ({elapsed})", name);
    }
}

/// Print a dry-run summary (plain text, no progress bars).
pub fn print_dry_run(result: &ApplyResult) {
    if result.actions.is_empty() && result.deferred.is_empty() {
        println!("No changes needed.");
        return;
    }

    for action in &result.actions {
        let verb = match action {
            vz_stack::Action::ServiceCreate { .. } => "create",
            vz_stack::Action::ServiceRecreate { .. } => "recreate",
            vz_stack::Action::ServiceRemove { .. } => "remove",
        };
        println!("  {verb:>10}  {}", action.service_name());
    }

    for deferred in &result.deferred {
        println!(
            "  deferred  {} (waiting on: {})",
            deferred.service_name,
            deferred.waiting_on.join(", "),
        );
    }

    println!(
        "\n{} action(s), {} deferred",
        result.actions.len(),
        result.deferred.len(),
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use vz_stack::{Action, ApplyResult, DeferredService};

    #[test]
    fn print_dry_run_empty() {
        let result = ApplyResult {
            actions: vec![],
            deferred: vec![],
        };
        // Should not panic.
        print_dry_run(&result);
    }

    #[test]
    fn print_dry_run_with_actions() {
        let result = ApplyResult {
            actions: vec![
                Action::ServiceCreate {
                    service_name: "web".into(),
                },
                Action::ServiceRemove {
                    service_name: "old".into(),
                },
            ],
            deferred: vec![DeferredService {
                service_name: "app".into(),
                waiting_on: vec!["db".into()],
            }],
        };
        print_dry_run(&result);
    }

    #[test]
    fn truncate_error_extracts_leaf() {
        let msg = "network error: create_in_sandbox failed: storage operation failed: unable to unpack layer";
        assert_eq!(truncate_error(msg), "unable to unpack layer");
    }

    #[test]
    fn truncate_error_truncates_sha256() {
        let msg = "unable to unpack layer sha256:e54bc7400b8c60e1d6cea4d86bfcd3725b446856ebdf665cfd6581b861931f66 using media type foo";
        let result = truncate_error(msg);
        assert!(result.contains("sha256:e54bc74\u{2026}"));
        assert!(!result.contains("e54bc7400b8c60e1d6"));
    }

    #[test]
    fn truncate_error_chain_and_sha256() {
        let msg = "network error: create_in_sandbox failed: storage operation failed: unable to unpack layer sha256:e54bc7400b8c60e1d6cea4d86bfcd3725b446856ebdf665cfd6581b861931f66 using media type application/vnd.oci.image.layer.v1.tar+gzip";
        let result = truncate_error(msg);
        assert!(result.starts_with("unable to unpack layer sha256:e54bc74\u{2026}"));
        assert!(result.chars().count() <= 100);
    }

    #[test]
    fn truncate_error_short_passthrough() {
        let msg = "health check failed";
        assert_eq!(truncate_error(msg), "health check failed");
    }

    #[test]
    fn truncate_error_caps_at_100_chars() {
        let msg = &"a".repeat(200);
        let result = truncate_error(msg);
        assert_eq!(result.chars().count(), 100);
        assert!(result.ends_with("..."));
    }
}
