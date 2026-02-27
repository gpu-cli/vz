//! `vz build` -- Build Dockerfiles into the local vz OCI store.

use std::collections::BTreeMap;
#[cfg(target_os = "macos")]
use std::collections::HashMap;
#[cfg(target_os = "macos")]
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

use clap::{Args, Subcommand, ValueEnum};
#[cfg(target_os = "macos")]
use console::style;
#[cfg(target_os = "macos")]
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use super::oci::ContainerOpts;

/// Build a Dockerfile or manage BuildKit cache.
#[derive(Args, Debug)]
pub struct BuildArgs {
    /// BuildKit-related subcommands.
    #[command(subcommand)]
    pub subcommand: Option<BuildSubcommand>,

    /// Build context directory.
    #[arg(default_value = ".")]
    pub context: PathBuf,

    /// Image name and optional tag (for example `myapp:latest`).
    #[arg(short = 't', long = "tag")]
    pub tag: Option<String>,

    /// Dockerfile path (relative to context unless absolute).
    #[arg(short = 'f', long = "file", default_value = "Dockerfile")]
    pub dockerfile: PathBuf,

    /// Multi-stage target to build.
    #[arg(long)]
    pub target: Option<String>,

    /// Build-time variable (`KEY=VALUE`). Can be repeated.
    #[arg(long = "build-arg", value_name = "KEY=VALUE")]
    pub build_args: Vec<String>,

    /// Build secret forwarded to BuildKit (`id=...,src=...`). Can be repeated.
    #[arg(long = "secret", value_name = "SPEC")]
    pub secrets: Vec<String>,

    /// Disable BuildKit cache.
    #[arg(long)]
    pub no_cache: bool,

    /// Push image to registry after build.
    #[arg(long)]
    pub push: bool,

    /// Explicit output specification (currently supports `type=oci,dest=<path>`).
    #[arg(short = 'o', long = "output")]
    pub output: Option<String>,

    /// Progress output mode.
    #[arg(long, value_enum, default_value_t = ProgressArg::Auto)]
    pub progress: ProgressArg,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Subcommand, Debug)]
pub enum BuildSubcommand {
    /// Manage BuildKit cache.
    Cache(BuildCacheArgs),
}

#[derive(Args, Debug)]
pub struct BuildCacheArgs {
    #[command(subcommand)]
    pub action: BuildCacheAction,
}

#[derive(Subcommand, Debug)]
pub enum BuildCacheAction {
    /// Show cache usage details.
    Du,
    /// Prune cache entries.
    Prune(BuildCachePruneArgs),
}

#[derive(Args, Debug)]
pub struct BuildCachePruneArgs {
    /// Remove all cache entries.
    #[arg(long)]
    pub all: bool,

    /// Keep cache newer than this duration (for example `24h`).
    #[arg(long = "keep-duration")]
    pub keep_duration: Option<String>,

    /// Keep this amount of storage (for example `5GB`).
    #[arg(long = "keep-storage")]
    pub keep_storage: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ProgressArg {
    #[default]
    Auto,
    Plain,
    Tty,
    #[value(name = "rawjson")]
    RawJson,
}

impl From<ProgressArg> for vz_oci_macos::buildkit::BuildProgress {
    fn from(value: ProgressArg) -> Self {
        match value {
            ProgressArg::Auto => Self::Auto,
            ProgressArg::Plain => Self::Plain,
            ProgressArg::Tty => Self::Tty,
            ProgressArg::RawJson => Self::RawJson,
        }
    }
}

/// Entry point for `vz build`.
pub async fn run(args: BuildArgs) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let config = super::oci::build_macos_runtime_config(&args.opts)?;

        if let Some(subcommand) = args.subcommand {
            return run_subcommand(config, subcommand).await;
        }

        let context_dir = expand_home_dir(&args.context);
        let tag = args.tag.unwrap_or_else(|| default_tag(&context_dir));
        let build_args = parse_build_args(&args.build_args)?;
        let secrets = parse_secrets(&args.secrets)?;
        let output = parse_output_mode(args.push, args.output.as_deref())?;
        let progress = args.progress;
        let stderr_is_tty = std::io::stderr().is_terminal();
        let (request_progress, ui_mode) = resolve_progress_mode(progress, stderr_is_tty);
        let display_tag = tag.clone();

        let request = vz_oci_macos::BuildRequest {
            context_dir,
            dockerfile: args.dockerfile,
            tag,
            target: args.target,
            cache_from: Vec::new(),
            build_args,
            secrets,
            no_cache: args.no_cache,
            output,
            progress: request_progress,
        };

        let mut streamer = BuildEventStreamer::new(ui_mode, display_tag)?;
        let result = vz_oci_macos::buildkit::build_image_with_events(&config, request, |event| {
            streamer.handle(event)
        })
        .await;
        streamer.finish(result.is_ok());
        let result = result?;
        match (&result.image_id, &result.output_path, result.pushed) {
            (Some(image_id), _, _) => println!("Built {} as {}", result.tag, image_id.0),
            (_, Some(path), _) => {
                println!(
                    "Built {} and wrote OCI archive to {}",
                    result.tag,
                    path.display()
                )
            }
            (_, _, true) => println!("Built and pushed {}", result.tag),
            _ => println!("Built {}", result.tag),
        }

        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = args;
        anyhow::bail!("`vz build` is currently supported only on macOS")
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildUiMode {
    FancyTui,
    PlainText,
    RawJsonStream,
}

#[cfg(target_os = "macos")]
fn resolve_progress_mode(
    progress: ProgressArg,
    stderr_is_tty: bool,
) -> (vz_oci_macos::buildkit::BuildProgress, BuildUiMode) {
    use vz_oci_macos::buildkit::BuildProgress;

    match progress {
        ProgressArg::Plain => (BuildProgress::Plain, BuildUiMode::PlainText),
        ProgressArg::RawJson => (BuildProgress::RawJson, BuildUiMode::RawJsonStream),
        ProgressArg::Tty => {
            if stderr_is_tty {
                (BuildProgress::RawJson, BuildUiMode::FancyTui)
            } else {
                (BuildProgress::Plain, BuildUiMode::PlainText)
            }
        }
        ProgressArg::Auto => {
            if stderr_is_tty {
                (BuildProgress::RawJson, BuildUiMode::FancyTui)
            } else {
                (BuildProgress::Plain, BuildUiMode::PlainText)
            }
        }
    }
}

#[cfg(target_os = "macos")]
struct BuildEventStreamer {
    mode: BuildUiMode,
    stdout: std::io::Stdout,
    stderr: std::io::Stderr,
    tui: Option<BuildTui>,
}

#[cfg(target_os = "macos")]
impl BuildEventStreamer {
    fn new(mode: BuildUiMode, tag: String) -> anyhow::Result<Self> {
        let tui = if matches!(mode, BuildUiMode::FancyTui) {
            Some(BuildTui::new(tag)?)
        } else {
            None
        };
        Ok(Self {
            mode,
            stdout: std::io::stdout(),
            stderr: std::io::stderr(),
            tui,
        })
    }

    fn handle(&mut self, event: vz_oci_macos::buildkit::BuildEvent) {
        match self.mode {
            BuildUiMode::FancyTui => self.handle_tui_event(event),
            BuildUiMode::PlainText => self.handle_text_event(event, true),
            BuildUiMode::RawJsonStream => self.handle_text_event(event, false),
        }
    }

    fn handle_text_event(&mut self, event: vz_oci_macos::buildkit::BuildEvent, show_status: bool) {
        use vz_oci_macos::buildkit::{BuildEvent, BuildLogStream};

        match event {
            BuildEvent::Status { message } => {
                if show_status {
                    let _ = writeln!(self.stderr, "==> {message}");
                    let _ = self.stderr.flush();
                }
            }
            BuildEvent::Output { stream, chunk } => match stream {
                BuildLogStream::Stdout => {
                    let _ = self.stdout.write_all(&chunk);
                    let _ = self.stdout.flush();
                }
                BuildLogStream::Stderr => {
                    let _ = self.stderr.write_all(&chunk);
                    let _ = self.stderr.flush();
                }
            },
            BuildEvent::RawJsonDecodeError { line, error } => {
                let _ = writeln!(
                    self.stderr,
                    "warning: failed to parse BuildKit rawjson line ({error}): {line}"
                );
                let _ = self.stderr.flush();
            }
            BuildEvent::SolveStatus { .. } => {}
        }
    }

    fn handle_tui_event(&mut self, event: vz_oci_macos::buildkit::BuildEvent) {
        let Some(tui) = self.tui.as_mut() else {
            return;
        };
        use vz_oci_macos::buildkit::BuildEvent;

        match event {
            BuildEvent::Status { message } => tui.on_status_message(&message),
            BuildEvent::Output { .. } => {}
            BuildEvent::SolveStatus { status } => tui.on_solve_status(status),
            BuildEvent::RawJsonDecodeError { line, error } => tui.on_decode_error(&line, &error),
        }
    }

    fn finish(&mut self, success: bool) {
        if let Some(tui) = self.tui.as_mut() {
            tui.finish(success);
            return;
        }
        let _ = self.stdout.flush();
        let _ = self.stderr.flush();
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildStepPhase {
    Running,
    Done,
    Cached,
    Failed,
}

#[cfg(target_os = "macos")]
struct BuildStepState {
    bar: ProgressBar,
    name: String,
    phase: BuildStepPhase,
    started_at: Instant,
    progress_current: i64,
    progress_total: i64,
    detail: Option<String>,
    finished: bool,
}

#[cfg(target_os = "macos")]
struct BuildTui {
    multi: MultiProgress,
    header: ProgressBar,
    step_order: Vec<String>,
    steps: HashMap<String, BuildStepState>,
    log_buffers: HashMap<(String, i64), Vec<u8>>,
    running_style: ProgressStyle,
    static_style: ProgressStyle,
    started_at: Instant,
    build_tag: String,
    phase: String,
    warnings: usize,
    decode_errors: usize,
    log_lines_emitted: usize,
    truncated_logs_notice_emitted: bool,
}

#[cfg(target_os = "macos")]
impl BuildTui {
    const MAX_LOG_LINES: usize = 400;

    fn new(tag: String) -> anyhow::Result<Self> {
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stderr());

        let tick_frames = &["⠋", "⠙", "⠚", "⠞", "⠖", "⠦", "⠴", "⠲", "⠳", "⠓"];

        let header = multi.add(ProgressBar::new_spinner());
        header.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(tick_frames)
                .template("{spinner:.cyan} {msg}")?,
        );
        header.enable_steady_tick(Duration::from_millis(90));

        let running_style = ProgressStyle::default_spinner()
            .tick_strings(tick_frames)
            .template("   {spinner:.cyan} {msg}")?;
        let static_style = ProgressStyle::default_spinner().template("   {msg}")?;

        let tui = Self {
            multi,
            header,
            step_order: Vec::new(),
            steps: HashMap::new(),
            log_buffers: HashMap::new(),
            running_style,
            static_style,
            started_at: Instant::now(),
            build_tag: tag,
            phase: "starting".to_string(),
            warnings: 0,
            decode_errors: 0,
            log_lines_emitted: 0,
            truncated_logs_notice_emitted: false,
        };
        tui.update_header();
        Ok(tui)
    }

    fn on_status_message(&mut self, message: &str) {
        self.phase = message.to_string();
        let _ = self.multi.println(format!(
            " {} {}",
            style(">").bold().cyan(),
            style(message).bold()
        ));
        self.update_header();
    }

    fn on_solve_status(&mut self, status: vz_oci_macos::buildkit::BuildkitSolveStatus) {
        for vertex in status.vertexes {
            self.apply_vertex(vertex);
        }
        for update in status.statuses {
            self.apply_status(update);
        }
        for log in status.logs {
            self.apply_log(log);
        }
        for warning in status.warnings {
            self.apply_warning(warning);
        }
        self.update_header();
    }

    fn on_decode_error(&mut self, line: &str, error: &str) {
        self.decode_errors = self.decode_errors.saturating_add(1);
        let _ = self.multi.println(format!(
            "      {} {} ({})",
            style("rawjson parse error").yellow(),
            style(error).yellow(),
            style(truncate_with_ellipsis(line, 120)).dim()
        ));
        self.update_header();
    }

    fn apply_vertex(&mut self, vertex: vz_oci_macos::buildkit::BuildkitVertex) {
        let step_id = status_or_name(&vertex.digest, &vertex.name, self.step_order.len() + 1);
        let step_name = sanitize_step_name(if vertex.name.is_empty() {
            &step_id
        } else {
            &vertex.name
        });
        self.ensure_step(&step_id, &step_name);

        if let Some(step) = self.steps.get_mut(&step_id) {
            if !vertex.name.is_empty() {
                step.name = sanitize_step_name(&vertex.name);
            }
            if vertex.cached {
                step.phase = BuildStepPhase::Cached;
                step.detail = None;
            } else if !vertex.error.is_empty() {
                step.phase = BuildStepPhase::Failed;
                step.detail = Some(truncate_with_ellipsis(vertex.error.trim(), 120));
            } else if vertex.completed.is_some() {
                if step.phase != BuildStepPhase::Failed {
                    step.phase = BuildStepPhase::Done;
                }
            } else if !step.finished {
                step.phase = BuildStepPhase::Running;
            }
        }
        self.render_step(&step_id);
    }

    fn apply_status(&mut self, update: vz_oci_macos::buildkit::BuildkitVertexStatus) {
        let step_id = status_or_name(&update.vertex, &update.name, self.step_order.len() + 1);
        let fallback_name = if update.name.is_empty() {
            step_id.as_str()
        } else {
            update.name.as_str()
        };
        self.ensure_step(&step_id, fallback_name);

        if let Some(step) = self.steps.get_mut(&step_id) {
            if !update.name.is_empty() {
                step.name = sanitize_step_name(&update.name);
                step.detail = Some(update.name.clone());
            }
            step.progress_current = update.current.max(0);
            step.progress_total = update.total.max(0);
            if update.completed.is_some() && step.phase == BuildStepPhase::Running {
                step.phase = BuildStepPhase::Done;
            }
        }
        self.render_step(&step_id);
    }

    fn apply_log(&mut self, log: vz_oci_macos::buildkit::BuildkitVertexLog) {
        if log.data.is_empty() {
            return;
        }
        let step_id = status_or_name(&log.vertex, "", self.step_order.len() + 1);
        self.ensure_step(&step_id, &step_id);

        let key = (step_id.clone(), log.stream);
        let mut completed_lines = Vec::new();
        {
            let buf = self.log_buffers.entry(key).or_default();
            buf.extend_from_slice(&log.data);
            while let Some(newline_idx) = buf.iter().position(|byte| *byte == b'\n') {
                let mut line = buf.drain(..=newline_idx).collect::<Vec<u8>>();
                line.pop();
                while line.last() == Some(&b'\r') {
                    line.pop();
                }
                completed_lines.push(line);
            }
        }

        for line in completed_lines {
            self.emit_log_line(&step_id, log.stream, &line);
        }
    }

    fn apply_warning(&mut self, warning: vz_oci_macos::buildkit::BuildkitVertexWarning) {
        self.warnings = self.warnings.saturating_add(1);
        let short = if warning.short.is_empty() {
            "build warning".to_string()
        } else {
            String::from_utf8_lossy(&warning.short).trim().to_string()
        };
        let line = format!("warning {}: {}", self.warnings, short);
        let _ = self
            .multi
            .println(format!("      {}", style(line).yellow()));
        if !warning.url.is_empty() {
            let _ = self
                .multi
                .println(format!("      {}", style(warning.url).cyan().dim()));
        }
    }

    fn ensure_step(&mut self, step_id: &str, name: &str) {
        if self.steps.contains_key(step_id) {
            return;
        }

        let bar = self.multi.add(ProgressBar::new_spinner());
        bar.set_style(self.running_style.clone());
        bar.enable_steady_tick(Duration::from_millis(90));
        self.step_order.push(step_id.to_string());
        self.steps.insert(
            step_id.to_string(),
            BuildStepState {
                bar,
                name: sanitize_step_name(name),
                phase: BuildStepPhase::Running,
                started_at: Instant::now(),
                progress_current: 0,
                progress_total: 0,
                detail: None,
                finished: false,
            },
        );
    }

    fn render_step(&mut self, step_id: &str) {
        let Some(index) = self.step_order.iter().position(|id| id == step_id) else {
            return;
        };
        let Some(step) = self.steps.get_mut(step_id) else {
            return;
        };

        let number = index + 1;
        let elapsed = format!("{:.1}s", step.started_at.elapsed().as_secs_f64());
        let progress_fragment = if step.progress_total > 0 {
            let pct = (step.progress_current as f64 / step.progress_total as f64 * 100.0)
                .clamp(0.0, 100.0);
            format!(
                " {:.0}% ({}/{})",
                pct, step.progress_current, step.progress_total
            )
        } else {
            String::new()
        };

        match step.phase {
            BuildStepPhase::Running => {
                step.bar.set_style(self.running_style.clone());
                step.bar.set_message(format!(
                    "{} {:02}. {:<64} {}{}",
                    style("RUN").cyan(),
                    number,
                    truncate_with_ellipsis(&step.name, 64),
                    style(elapsed).dim(),
                    style(progress_fragment).dim()
                ));
            }
            BuildStepPhase::Done => {
                step.bar.set_style(self.static_style.clone());
                step.bar.set_message(format!(
                    "{} {:02}. {:<64} {}",
                    style("OK ").green(),
                    number,
                    truncate_with_ellipsis(&step.name, 64),
                    style(elapsed).dim()
                ));
                if !step.finished {
                    step.finished = true;
                    step.bar.finish();
                }
            }
            BuildStepPhase::Cached => {
                step.bar.set_style(self.static_style.clone());
                step.bar.set_message(format!(
                    "{} {:02}. {:<64} {}",
                    style("OK ").green(),
                    number,
                    truncate_with_ellipsis(&step.name, 64),
                    style("cached").dim()
                ));
                if !step.finished {
                    step.finished = true;
                    step.bar.finish();
                }
            }
            BuildStepPhase::Failed => {
                step.bar.set_style(self.static_style.clone());
                let detail = step.detail.as_deref().unwrap_or("build step failed");
                step.bar.set_message(format!(
                    "{} {:02}. {:<64} {}",
                    style("ERR").red(),
                    number,
                    truncate_with_ellipsis(&step.name, 64),
                    style(truncate_with_ellipsis(detail, 90)).red()
                ));
                if !step.finished {
                    step.finished = true;
                    step.bar.finish();
                }
            }
        }
    }

    fn emit_log_line(&mut self, step_id: &str, stream: i64, line: &[u8]) {
        if line.is_empty() {
            return;
        }

        if self.log_lines_emitted >= Self::MAX_LOG_LINES {
            if !self.truncated_logs_notice_emitted {
                let _ = self.multi.println(format!(
                    "      {}",
                    style("... log output truncated for readability ...").dim()
                ));
                self.truncated_logs_notice_emitted = true;
            }
            return;
        }

        let stream_tag = match stream {
            2 => "stderr",
            _ => "stdout",
        };
        let label = self
            .step_order
            .iter()
            .position(|id| id == step_id)
            .map(|idx| format!("{:02}", idx + 1))
            .unwrap_or_else(|| "--".to_string());
        let rendered = String::from_utf8_lossy(line).trim().to_string();
        if rendered.is_empty() {
            return;
        }
        self.log_lines_emitted = self.log_lines_emitted.saturating_add(1);
        let _ = self.multi.println(format!(
            "      {} {} {}",
            style(label).dim(),
            style(stream_tag).dim(),
            rendered
        ));
    }

    fn flush_log_buffers(&mut self) {
        let mut leftovers = Vec::new();
        for ((step_id, stream), mut bytes) in std::mem::take(&mut self.log_buffers) {
            while bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
            if !bytes.is_empty() {
                leftovers.push((step_id, stream, bytes));
            }
        }
        for (step_id, stream, bytes) in leftovers {
            self.emit_log_line(&step_id, stream, &bytes);
        }
    }

    fn finish(&mut self, success: bool) {
        self.flush_log_buffers();

        let ids = self.step_order.clone();
        for step_id in ids {
            if let Some(step) = self.steps.get_mut(&step_id)
                && step.phase == BuildStepPhase::Running
            {
                if success {
                    step.phase = BuildStepPhase::Done;
                } else {
                    step.phase = BuildStepPhase::Failed;
                    if step.detail.is_none() {
                        step.detail = Some("build interrupted".to_string());
                    }
                }
            }
            self.render_step(&step_id);
        }

        let (total, done, active, cached, failed) = self.step_counters();
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let status = if success && failed == 0 {
            style("COMPLETE").green().bold().to_string()
        } else {
            style("FAILED").red().bold().to_string()
        };
        self.header.set_style(self.static_style.clone());
        self.header.set_message(format!(
            "{} {} | steps {}/{} | active {} | cached {} | failed {} | warnings {} | rawjson-errors {} | {:.1}s",
            status,
            self.build_tag,
            done,
            total,
            active,
            cached,
            failed,
            self.warnings,
            self.decode_errors,
            elapsed
        ));
        self.header.finish();
    }

    fn update_header(&self) {
        let (total, done, active, cached, failed) = self.step_counters();
        let elapsed = self.started_at.elapsed().as_secs_f64();
        self.header.set_message(format!(
            "{} {} | {}/{} complete | active {} | cached {} | failed {} | warnings {} | {:.1}s",
            style("BUILD").bold().cyan(),
            self.phase,
            done,
            total,
            active,
            cached,
            failed,
            self.warnings,
            elapsed
        ));
    }

    fn step_counters(&self) -> (usize, usize, usize, usize, usize) {
        let mut done = 0usize;
        let mut active = 0usize;
        let mut cached = 0usize;
        let mut failed = 0usize;
        for step in self.steps.values() {
            match step.phase {
                BuildStepPhase::Running => active = active.saturating_add(1),
                BuildStepPhase::Done => done = done.saturating_add(1),
                BuildStepPhase::Cached => {
                    done = done.saturating_add(1);
                    cached = cached.saturating_add(1);
                }
                BuildStepPhase::Failed => failed = failed.saturating_add(1),
            }
        }
        (self.step_order.len(), done, active, cached, failed)
    }
}

#[cfg(target_os = "macos")]
fn status_or_name(digest: &str, name: &str, fallback_idx: usize) -> String {
    if !digest.trim().is_empty() {
        digest.to_string()
    } else if !name.trim().is_empty() {
        name.trim().to_string()
    } else {
        format!("step-{fallback_idx}")
    }
}

#[cfg(target_os = "macos")]
fn sanitize_step_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "unnamed step".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(target_os = "macos")]
fn truncate_with_ellipsis(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return value.chars().take(max_chars).collect();
    }
    let mut out = value.chars().take(max_chars - 3).collect::<String>();
    out.push_str("...");
    out
}

#[cfg(target_os = "macos")]
async fn run_subcommand(
    config: vz_oci_macos::RuntimeConfig,
    subcommand: BuildSubcommand,
) -> anyhow::Result<()> {
    match subcommand {
        BuildSubcommand::Cache(cache) => match cache.action {
            BuildCacheAction::Du => {
                let output = vz_oci_macos::buildkit::cache_disk_usage(&config).await?;
                if output.trim().is_empty() {
                    println!("No BuildKit cache entries");
                } else {
                    println!("{output}");
                }
            }
            BuildCacheAction::Prune(prune) => {
                let output = vz_oci_macos::buildkit::cache_prune(
                    &config,
                    vz_oci_macos::buildkit::CachePruneOptions {
                        all: prune.all,
                        keep_duration: prune.keep_duration,
                        keep_storage: prune.keep_storage,
                    },
                )
                .await?;
                if output.trim().is_empty() {
                    println!("BuildKit cache prune complete");
                } else {
                    println!("{output}");
                }
            }
        },
    }
    Ok(())
}

fn parse_build_args(values: &[String]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for raw in values {
        let Some((key, value)) = raw.split_once('=') else {
            anyhow::bail!("invalid --build-arg `{raw}` (expected KEY=VALUE)");
        };
        if key.trim().is_empty() {
            anyhow::bail!("invalid --build-arg `{raw}` (empty key)");
        }
        parsed.insert(key.to_string(), value.to_string());
    }
    Ok(parsed)
}

fn parse_output_mode(
    push: bool,
    output_spec: Option<&str>,
) -> anyhow::Result<vz_oci_macos::buildkit::BuildOutput> {
    if push && output_spec.is_some() {
        anyhow::bail!("--push cannot be combined with --output");
    }

    if push {
        return Ok(vz_oci_macos::buildkit::BuildOutput::RegistryPush);
    }

    let Some(output_spec) = output_spec else {
        return Ok(vz_oci_macos::buildkit::BuildOutput::VzStore);
    };

    let mut fields = BTreeMap::new();
    for chunk in output_spec.split(',') {
        let Some((key, value)) = chunk.split_once('=') else {
            anyhow::bail!("invalid --output field `{chunk}` (expected key=value)");
        };
        fields.insert(key.trim().to_string(), value.trim().to_string());
    }

    let output_type = fields
        .remove("type")
        .ok_or_else(|| anyhow::anyhow!("--output requires `type=` field"))?;
    if output_type != "oci" {
        anyhow::bail!("unsupported --output type `{output_type}` (only `type=oci` is supported)");
    }

    let dest = fields
        .remove("dest")
        .ok_or_else(|| anyhow::anyhow!("--output requires `dest=` field for `type=oci`"))?;

    if !fields.is_empty() {
        let extras = fields.keys().cloned().collect::<Vec<_>>().join(", ");
        anyhow::bail!("unsupported --output field(s): {extras}");
    }

    Ok(vz_oci_macos::buildkit::BuildOutput::OciTar {
        dest: expand_home_dir(Path::new(&dest)),
    })
}

fn parse_secrets(values: &[String]) -> anyhow::Result<Vec<String>> {
    let mut parsed = Vec::with_capacity(values.len());
    for raw in values {
        if !raw.contains('=') {
            anyhow::bail!(
                "invalid --secret `{raw}` (expected key=value pairs like id=...,src=...)"
            );
        }
        parsed.push(raw.clone());
    }
    Ok(parsed)
}

fn default_tag(context: &Path) -> String {
    let stem = context
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("image");
    let mut out = String::new();
    for ch in stem.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    if out.trim_matches('-').is_empty() {
        "image:latest".to_string()
    } else {
        format!("{}:latest", out.trim_matches('-'))
    }
}

fn expand_home_dir(path: &Path) -> PathBuf {
    if let Some(path_str) = path.to_str() {
        if let Some(rest) = path_str.strip_prefix("~/")
            && let Some(home) = std::env::var_os("HOME")
        {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

// ── BuildKit stderr JSON parser ───────────────────────────────────
//
// Extracts meaningful progress information from BuildKit rawjson
// stderr lines, filtering out noise and presenting clean status.

/// Status of an individual build step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStepStatus {
    /// Step is currently executing.
    Running,
    /// Step completed successfully.
    Complete,
    /// Step was satisfied from cache.
    Cached,
    /// Step failed with an error.
    Error,
}

/// Parsed progress information from a BuildKit rawjson line.
///
/// Each line of BuildKit `--progress=rawjson` output is a JSON object
/// containing vertex, status, log, and warning arrays. This struct
/// distills that into the most relevant progress information.
#[derive(Debug, Clone)]
pub struct BuildProgressInfo {
    /// Human-readable step name (e.g. "[1/3] FROM docker.io/library/alpine:3.20").
    pub step: String,
    /// Current status of the step.
    pub status: BuildStepStatus,
    /// Elapsed duration of the step, if start and completion times are available.
    pub duration: Option<Duration>,
    /// Error message, if the step failed.
    pub error: Option<String>,
}

/// Parse a single line of BuildKit rawjson stderr output into build progress.
///
/// Returns `None` if the line is not valid BuildKit JSON or contains no
/// meaningful progress information (e.g. only log data, empty vertices).
///
/// # Examples
///
/// ```ignore
/// let line = r#"{"vertexes":[{"digest":"sha256:abc","name":"[1/2] FROM alpine","cached":true}]}"#;
/// if let Some(progress) = parse_buildkit_output(line) {
///     assert_eq!(progress.status, BuildStepStatus::Cached);
/// }
/// ```
pub fn parse_buildkit_output(line: &str) -> Option<BuildProgressInfo> {
    let status: vz_oci_macos::buildkit::BuildkitSolveStatus = serde_json::from_str(line).ok()?;

    // Find the most informative vertex in this line.
    // Prefer vertices with names (build steps) over anonymous ones.
    let vertex = status
        .vertexes
        .iter()
        .filter(|v| !v.name.trim().is_empty())
        .last()
        .or_else(|| status.vertexes.last())?;

    let step_name = if vertex.name.trim().is_empty() {
        vertex.digest.clone()
    } else {
        vertex.name.trim().to_string()
    };

    let step_status = if !vertex.error.is_empty() {
        BuildStepStatus::Error
    } else if vertex.cached {
        BuildStepStatus::Cached
    } else if vertex.completed.is_some() {
        BuildStepStatus::Complete
    } else {
        BuildStepStatus::Running
    };

    let duration = match (&vertex.started, &vertex.completed) {
        (Some(start), Some(end)) => parse_duration_between(start, end),
        _ => None,
    };

    let error = if vertex.error.is_empty() {
        None
    } else {
        Some(vertex.error.trim().to_string())
    };

    Some(BuildProgressInfo {
        step: step_name,
        status: step_status,
        duration,
        error,
    })
}

/// Try to compute the duration between two RFC 3339 timestamps.
///
/// Returns `None` if either timestamp cannot be parsed. This is best-effort
/// since BuildKit timestamps may use varying precision.
fn parse_duration_between(start: &str, end: &str) -> Option<Duration> {
    // Simple ISO 8601 / RFC 3339 parsing. BuildKit emits timestamps like
    // "2026-02-23T13:00:00.123456789Z". We use string parsing rather than
    // pulling in chrono to keep dependencies minimal.
    let start_nanos = parse_rfc3339_nanos(start)?;
    let end_nanos = parse_rfc3339_nanos(end)?;
    let elapsed = end_nanos.checked_sub(start_nanos)?;
    Some(Duration::from_nanos(elapsed as u64))
}

/// Parse an RFC 3339 timestamp to nanoseconds since epoch.
///
/// Handles timestamps with or without fractional seconds.
fn parse_rfc3339_nanos(ts: &str) -> Option<u128> {
    // Strip trailing 'Z' or timezone offset for simplicity.
    let ts = ts.trim();
    let (datetime_part, _tz) = if let Some(pos) = ts.rfind('Z') {
        (&ts[..pos], "Z")
    } else if let Some(pos) = ts.rfind('+') {
        // Skip the '+' in the fractional part
        if pos > 10 {
            (&ts[..pos], &ts[pos..])
        } else {
            return None;
        }
    } else {
        return None;
    };

    // Split on 'T' to get date and time.
    let (date_str, time_str) = datetime_part.split_once('T')?;

    // Parse date: YYYY-MM-DD
    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    // Approximate days since epoch (good enough for duration computation).
    let days_since_epoch = (year - 1970) * 365 + (year - 1969) / 4 + day_of_year(month, day) - 1;

    // Parse time: HH:MM:SS[.fractional]
    let (time_main, frac) = if let Some((main, frac)) = time_str.split_once('.') {
        (main, Some(frac))
    } else {
        (time_str, None)
    };

    let time_parts: Vec<&str> = time_main.split(':').collect();
    if time_parts.len() != 3 {
        return None;
    }
    let hours: i64 = time_parts[0].parse().ok()?;
    let minutes: i64 = time_parts[1].parse().ok()?;
    let seconds: i64 = time_parts[2].parse().ok()?;

    let total_secs = days_since_epoch * 86400 + hours * 3600 + minutes * 60 + seconds;

    let frac_nanos: u64 = if let Some(frac_str) = frac {
        // Pad or truncate to 9 digits.
        let padded = format!("{:0<9}", frac_str);
        padded[..9].parse().unwrap_or(0)
    } else {
        0
    };

    Some(total_secs as u128 * 1_000_000_000 + frac_nanos as u128)
}

/// Approximate day-of-year (1-indexed) for duration computation.
fn day_of_year(month: i64, day: i64) -> i64 {
    let days_before_month = match month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => 0,
    };
    days_before_month + day
}

/// Format a [`BuildProgressInfo`] as a single clean line suitable for stderr.
///
/// Example output:
/// ```text
/// [CACHED] [1/3] FROM alpine:3.20
/// [OK 1.2s] [2/3] RUN apt-get update
/// [ERROR] [3/3] COPY . /app -- file not found
/// [RUN] [1/1] Building application
/// ```
pub fn format_build_progress(info: &BuildProgressInfo) -> String {
    let prefix = match info.status {
        BuildStepStatus::Running => "[RUN]".to_string(),
        BuildStepStatus::Complete => {
            if let Some(dur) = info.duration {
                format!("[OK {:.1}s]", dur.as_secs_f64())
            } else {
                "[OK]".to_string()
            }
        }
        BuildStepStatus::Cached => "[CACHED]".to_string(),
        BuildStepStatus::Error => "[ERROR]".to_string(),
    };

    if let Some(err) = &info.error {
        format!("{prefix} {} -- {err}", info.step)
    } else {
        format!("{prefix} {}", info.step)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use clap::ValueEnum;

    #[test]
    fn parse_build_args_supports_multiple_values() {
        let values = vec!["A=1".to_string(), "B=two".to_string()];
        let parsed = parse_build_args(&values).unwrap();
        assert_eq!(parsed.get("A").unwrap(), "1");
        assert_eq!(parsed.get("B").unwrap(), "two");
    }

    #[test]
    fn parse_build_args_rejects_missing_equals() {
        let values = vec!["BROKEN".to_string()];
        let err = parse_build_args(&values).unwrap_err();
        assert!(err.to_string().contains("expected KEY=VALUE"));
    }

    #[test]
    fn parse_secrets_accepts_specs() {
        let values = vec![
            "id=npmrc,src=.npmrc".to_string(),
            "id=token,env=NPM_TOKEN".to_string(),
        ];
        let parsed = parse_secrets(&values).unwrap();
        assert_eq!(parsed, values);
    }

    #[test]
    fn parse_secrets_rejects_missing_equals() {
        let values = vec!["npmrc".to_string()];
        let err = parse_secrets(&values).unwrap_err();
        assert!(err.to_string().contains("expected key=value"));
    }

    #[test]
    fn parse_output_mode_defaults_to_vz_store() {
        let parsed = parse_output_mode(false, None).unwrap();
        assert!(matches!(
            parsed,
            vz_oci_macos::buildkit::BuildOutput::VzStore
        ));
    }

    #[test]
    fn parse_output_mode_supports_push() {
        let parsed = parse_output_mode(true, None).unwrap();
        assert!(matches!(
            parsed,
            vz_oci_macos::buildkit::BuildOutput::RegistryPush
        ));
    }

    #[test]
    fn parse_output_mode_supports_oci_tar() {
        let parsed = parse_output_mode(false, Some("type=oci,dest=./image.tar")).unwrap();
        match parsed {
            vz_oci_macos::buildkit::BuildOutput::OciTar { dest } => {
                assert_eq!(dest, PathBuf::from("./image.tar"));
            }
            _ => panic!("unexpected output mode"),
        }
    }

    #[test]
    fn parse_output_mode_rejects_push_and_output() {
        let err = parse_output_mode(true, Some("type=oci,dest=./image.tar")).unwrap_err();
        assert!(err.to_string().contains("cannot be combined"));
    }

    #[test]
    fn default_tag_uses_context_directory_name() {
        let tag = default_tag(Path::new("/tmp/My App"));
        assert_eq!(tag, "my-app:latest");
    }

    #[test]
    fn progress_arg_supports_rawjson_value() {
        let parsed = ProgressArg::from_str("rawjson", true).unwrap();
        assert!(matches!(parsed, ProgressArg::RawJson));
    }

    #[test]
    fn progress_arg_maps_to_buildkit_progress() {
        let mapped: vz_oci_macos::buildkit::BuildProgress = ProgressArg::RawJson.into();
        assert!(matches!(
            mapped,
            vz_oci_macos::buildkit::BuildProgress::RawJson
        ));
    }

    #[test]
    fn resolve_progress_mode_prefers_tui_for_auto_tty() {
        let (progress, mode) = resolve_progress_mode(ProgressArg::Auto, true);
        assert!(matches!(
            progress,
            vz_oci_macos::buildkit::BuildProgress::RawJson
        ));
        assert!(matches!(mode, BuildUiMode::FancyTui));
    }

    #[test]
    fn resolve_progress_mode_uses_plain_for_non_tty_auto() {
        let (progress, mode) = resolve_progress_mode(ProgressArg::Auto, false);
        assert!(matches!(
            progress,
            vz_oci_macos::buildkit::BuildProgress::Plain
        ));
        assert!(matches!(mode, BuildUiMode::PlainText));
    }

    // ── parse_buildkit_output tests ───────────────────────────────

    #[test]
    fn parse_buildkit_output_cached_vertex() {
        let line = r#"{"vertexes":[{"digest":"sha256:abc","name":"[1/2] FROM docker.io/library/alpine:3.20","cached":true,"started":"2026-02-23T13:00:00Z","completed":"2026-02-23T13:00:00Z"}]}"#;
        let info = parse_buildkit_output(line).unwrap();
        assert_eq!(info.step, "[1/2] FROM docker.io/library/alpine:3.20");
        assert_eq!(info.status, BuildStepStatus::Cached);
        assert!(info.error.is_none());
    }

    #[test]
    fn parse_buildkit_output_completed_vertex_with_duration() {
        let line = r#"{"vertexes":[{"digest":"sha256:def","name":"[2/2] RUN apt-get update","started":"2026-02-23T13:00:00Z","completed":"2026-02-23T13:00:02.5Z"}]}"#;
        let info = parse_buildkit_output(line).unwrap();
        assert_eq!(info.step, "[2/2] RUN apt-get update");
        assert_eq!(info.status, BuildStepStatus::Complete);
        let duration = info.duration.unwrap();
        assert!((duration.as_secs_f64() - 2.5).abs() < 0.01);
    }

    #[test]
    fn parse_buildkit_output_running_vertex() {
        let line = r#"{"vertexes":[{"digest":"sha256:ghi","name":"[1/1] RUN make build","started":"2026-02-23T13:00:00Z"}]}"#;
        let info = parse_buildkit_output(line).unwrap();
        assert_eq!(info.status, BuildStepStatus::Running);
        assert!(info.duration.is_none());
    }

    #[test]
    fn parse_buildkit_output_error_vertex() {
        let line = r#"{"vertexes":[{"digest":"sha256:jkl","name":"[3/3] COPY . /app","error":"file not found","started":"2026-02-23T13:00:00Z"}]}"#;
        let info = parse_buildkit_output(line).unwrap();
        assert_eq!(info.status, BuildStepStatus::Error);
        assert_eq!(info.error.as_deref(), Some("file not found"));
    }

    #[test]
    fn parse_buildkit_output_invalid_json_returns_none() {
        assert!(parse_buildkit_output("not json at all").is_none());
        assert!(parse_buildkit_output("").is_none());
        assert!(parse_buildkit_output("{}").is_none()); // empty vertexes
    }

    #[test]
    fn parse_buildkit_output_logs_only_returns_none() {
        // Lines with only logs and no vertices should return None.
        let line = r#"{"logs":[{"vertex":"sha256:abc","stream":1,"data":"aGVsbG8K"}]}"#;
        assert!(parse_buildkit_output(line).is_none());
    }

    #[test]
    fn parse_buildkit_output_prefers_named_vertex() {
        let line = r#"{"vertexes":[{"digest":"sha256:anon","name":""},{"digest":"sha256:named","name":"[1/1] RUN echo hello"}]}"#;
        let info = parse_buildkit_output(line).unwrap();
        assert_eq!(info.step, "[1/1] RUN echo hello");
    }

    #[test]
    fn format_build_progress_running() {
        let info = BuildProgressInfo {
            step: "[1/1] Building".to_string(),
            status: BuildStepStatus::Running,
            duration: None,
            error: None,
        };
        assert_eq!(format_build_progress(&info), "[RUN] [1/1] Building");
    }

    #[test]
    fn format_build_progress_complete_with_duration() {
        let info = BuildProgressInfo {
            step: "[1/1] RUN make".to_string(),
            status: BuildStepStatus::Complete,
            duration: Some(Duration::from_secs_f64(3.45)),
            error: None,
        };
        assert_eq!(format_build_progress(&info), "[OK 3.5s] [1/1] RUN make");
    }

    #[test]
    fn format_build_progress_cached() {
        let info = BuildProgressInfo {
            step: "FROM alpine".to_string(),
            status: BuildStepStatus::Cached,
            duration: None,
            error: None,
        };
        assert_eq!(format_build_progress(&info), "[CACHED] FROM alpine");
    }

    #[test]
    fn format_build_progress_error() {
        let info = BuildProgressInfo {
            step: "COPY . /app".to_string(),
            status: BuildStepStatus::Error,
            duration: None,
            error: Some("file not found".to_string()),
        };
        assert_eq!(
            format_build_progress(&info),
            "[ERROR] COPY . /app -- file not found"
        );
    }

    #[test]
    fn build_step_status_debug_and_clone() {
        let status = BuildStepStatus::Running;
        let cloned = status;
        assert_eq!(cloned, BuildStepStatus::Running);
        let _debug = format!("{:?}", status);
    }

    #[test]
    fn build_progress_info_clone() {
        let info = BuildProgressInfo {
            step: "test".to_string(),
            status: BuildStepStatus::Complete,
            duration: Some(Duration::from_secs(1)),
            error: None,
        };
        let cloned = info.clone();
        assert_eq!(cloned.step, "test");
        assert_eq!(cloned.status, BuildStepStatus::Complete);
    }
}
