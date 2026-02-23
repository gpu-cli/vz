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
            build_args,
            secrets,
            no_cache: args.no_cache,
            output,
            progress: request_progress,
        };

        let mut streamer = BuildEventStreamer::new(ui_mode, display_tag);
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

        return Ok(());
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
    fn new(mode: BuildUiMode, tag: String) -> Self {
        let tui = if matches!(mode, BuildUiMode::FancyTui) {
            Some(BuildTui::new(tag))
        } else {
            None
        };
        Self {
            mode,
            stdout: std::io::stdout(),
            stderr: std::io::stderr(),
            tui,
        }
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

    fn new(tag: String) -> Self {
        let multi = MultiProgress::new();
        multi.set_draw_target(ProgressDrawTarget::stderr());

        let tick_frames = &["⠋", "⠙", "⠚", "⠞", "⠖", "⠦", "⠴", "⠲", "⠳", "⠓"];

        let header = multi.add(ProgressBar::new_spinner());
        header.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(tick_frames)
                .template("{spinner:.cyan} {msg}")
                .expect("valid template"),
        );
        header.enable_steady_tick(Duration::from_millis(90));

        let running_style = ProgressStyle::default_spinner()
            .tick_strings(tick_frames)
            .template("   {spinner:.cyan} {msg}")
            .expect("valid template");
        let static_style = ProgressStyle::default_spinner()
            .template("   {msg}")
            .expect("valid template");

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
        tui
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
}
