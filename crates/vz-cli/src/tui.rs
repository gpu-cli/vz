//! Terminal UI dashboard for `vz stack`.
//!
//! Provides a ratatui-based three-tab dashboard (Services, Events, Logs)
//! that polls a [`StateStore`] for real-time service status and events.
//! The TUI can be launched either inline during `vz stack up` (foreground
//! mode) or standalone via `vz stack dashboard <name>`.

use std::collections::HashMap;
use std::io::{self, IsTerminal, Stdout};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::*;

use vz_stack::{
    EventRecord, ServiceObservedState, ServicePhase, StackEvent, StackSpec, StateStore,
};

// ── Types ──────────────────────────────────────────────────────────

/// Active tab in the TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    /// Service status table.
    Services,
    /// Full event history.
    Events,
    /// Per-service log output.
    Logs,
}

impl Tab {
    /// All tab variants in display order.
    const ALL: [Tab; 3] = [Tab::Services, Tab::Events, Tab::Logs];

    /// Display label for the tab.
    fn label(self) -> &'static str {
        match self {
            Tab::Services => "Services",
            Tab::Events => "Events",
            Tab::Logs => "Logs",
        }
    }

    /// Advance to the next tab (wraps around).
    fn next(self) -> Tab {
        match self {
            Tab::Services => Tab::Events,
            Tab::Events => Tab::Logs,
            Tab::Logs => Tab::Services,
        }
    }
}

/// TUI application state.
pub struct App {
    stack_name: String,
    spec: StackSpec,
    store: StateStore,

    // Tab state
    active_tab: Tab,

    // Services tab
    services: Vec<ServiceObservedState>,
    selected_service: usize,

    // Events tab
    events: Vec<EventRecord>,
    last_event_id: i64,
    event_scroll: usize,
    event_auto_scroll: bool,

    // Logs tab
    logs: HashMap<String, String>,
    selected_log_service: usize,
    log_scroll: usize,

    // UI state
    show_help: bool,
    should_quit: bool,

    // Bottom event strip (last 5 events)
    recent_events: Vec<EventRecord>,
}

impl App {
    /// Create a new TUI application.
    pub fn new(stack_name: String, spec: StackSpec, store: StateStore) -> Self {
        let service_names: Vec<String> = spec.services.iter().map(|s| s.name.clone()).collect();
        let logs: HashMap<String, String> = service_names
            .iter()
            .map(|n| (n.clone(), String::new()))
            .collect();

        Self {
            stack_name,
            spec,
            store,
            active_tab: Tab::Services,
            services: Vec::new(),
            selected_service: 0,
            events: Vec::new(),
            last_event_id: 0,
            event_scroll: 0,
            event_auto_scroll: true,
            logs,
            selected_log_service: 0,
            log_scroll: 0,
            show_help: false,
            should_quit: false,
            recent_events: Vec::new(),
        }
    }

    /// Poll the state store for updated service states and new events.
    fn refresh_data(&mut self) {
        // Poll observed state.
        if let Ok(observed) = self.store.load_observed_state(&self.stack_name) {
            self.services = observed;
        }

        // Poll new events.
        if let Ok(new_events) = self
            .store
            .load_events_since(&self.stack_name, self.last_event_id)
        {
            if let Some(last) = new_events.last() {
                self.last_event_id = last.id;
            }
            self.events.extend(new_events);

            // Keep last 5 for the bottom strip.
            let len = self.events.len();
            self.recent_events = self.events[len.saturating_sub(5)..].to_vec();

            // Auto-scroll events tab if enabled.
            if self.event_auto_scroll && !self.events.is_empty() {
                self.event_scroll = self.events.len().saturating_sub(1);
            }
        }
    }

    /// Handle a key press event.
    fn handle_key(&mut self, key: event::KeyEvent) {
        // Global keys.
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return;
        }

        if self.show_help {
            // Any key dismisses the help overlay.
            self.show_help = false;
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Tab => self.active_tab = self.active_tab.next(),
            KeyCode::Char('1') => self.active_tab = Tab::Services,
            KeyCode::Char('2') => self.active_tab = Tab::Events,
            KeyCode::Char('3') => self.active_tab = Tab::Logs,

            // Navigation
            KeyCode::Char('j') | KeyCode::Down => self.navigate_down(),
            KeyCode::Char('k') | KeyCode::Up => self.navigate_up(),
            KeyCode::Char('g') => self.jump_to_top(),
            KeyCode::Char('G') => self.jump_to_bottom(),

            _ => {}
        }
    }

    /// Navigate down in the current tab's list.
    fn navigate_down(&mut self) {
        match self.active_tab {
            Tab::Services => {
                if !self.services.is_empty() {
                    self.selected_service =
                        (self.selected_service + 1).min(self.services.len().saturating_sub(1));
                }
            }
            Tab::Events => {
                if !self.events.is_empty() {
                    self.event_scroll =
                        (self.event_scroll + 1).min(self.events.len().saturating_sub(1));
                    self.event_auto_scroll =
                        self.event_scroll >= self.events.len().saturating_sub(1);
                }
            }
            Tab::Logs => {
                let service_count = self.spec.services.len();
                if service_count > 0 {
                    self.selected_log_service =
                        (self.selected_log_service + 1).min(service_count.saturating_sub(1));
                    self.log_scroll = 0;
                }
            }
        }
    }

    /// Navigate up in the current tab's list.
    fn navigate_up(&mut self) {
        match self.active_tab {
            Tab::Services => {
                self.selected_service = self.selected_service.saturating_sub(1);
            }
            Tab::Events => {
                self.event_scroll = self.event_scroll.saturating_sub(1);
                self.event_auto_scroll = false;
            }
            Tab::Logs => {
                self.selected_log_service = self.selected_log_service.saturating_sub(1);
                self.log_scroll = 0;
            }
        }
    }

    /// Jump to the top of the current list.
    fn jump_to_top(&mut self) {
        match self.active_tab {
            Tab::Services => self.selected_service = 0,
            Tab::Events => {
                self.event_scroll = 0;
                self.event_auto_scroll = false;
            }
            Tab::Logs => {
                self.log_scroll = 0;
            }
        }
    }

    /// Jump to the bottom of the current list.
    fn jump_to_bottom(&mut self) {
        match self.active_tab {
            Tab::Services => {
                self.selected_service = self.services.len().saturating_sub(1);
            }
            Tab::Events => {
                self.event_scroll = self.events.len().saturating_sub(1);
                self.event_auto_scroll = true;
            }
            Tab::Logs => {
                // Scroll to end of current service log.
                if let Some(name) = self.current_log_service_name() {
                    if let Some(log) = self.logs.get(&name) {
                        let lines = log.lines().count();
                        self.log_scroll = lines.saturating_sub(1);
                    }
                }
            }
        }
    }

    /// Get the service name for the currently selected log service.
    fn current_log_service_name(&self) -> Option<String> {
        self.spec
            .services
            .get(self.selected_log_service)
            .map(|s| s.name.clone())
    }
}

// ── Color mapping ──────────────────────────────────────────────────

/// Map a service phase to a terminal color.
fn phase_color(phase: &ServicePhase) -> Color {
    match phase {
        ServicePhase::Running => Color::Green,
        ServicePhase::Failed => Color::Red,
        ServicePhase::Pending => Color::Yellow,
        ServicePhase::Creating => Color::Cyan,
        ServicePhase::Stopping => Color::DarkGray,
        ServicePhase::Stopped => Color::DarkGray,
    }
}

/// Map a stack event to a terminal color.
fn event_color(event: &StackEvent) -> Color {
    match event {
        StackEvent::StackApplyStarted { .. } => Color::Blue,
        StackEvent::StackApplyCompleted { .. } => Color::Green,
        StackEvent::StackApplyFailed { .. } => Color::Red,
        StackEvent::ServiceCreating { .. } => Color::Cyan,
        StackEvent::ServiceReady { .. } => Color::Green,
        StackEvent::ServiceStopping { .. } => Color::DarkGray,
        StackEvent::ServiceStopped { .. } => Color::DarkGray,
        StackEvent::ServiceFailed { .. } => Color::Red,
        StackEvent::PortConflict { .. } => Color::Red,
        StackEvent::VolumeCreated { .. } => Color::Blue,
        StackEvent::StackDestroyed { .. } => Color::Yellow,
        StackEvent::HealthCheckPassed { .. } => Color::Green,
        StackEvent::HealthCheckFailed { .. } => Color::Yellow,
        StackEvent::DependencyBlocked { .. } => Color::Magenta,
    }
}

/// Format a stack event into a one-line summary.
fn format_event_summary(event: &StackEvent) -> String {
    match event {
        StackEvent::StackApplyStarted { services_count, .. } => {
            format!("StackApplyStarted ({services_count} services)")
        }
        StackEvent::StackApplyCompleted {
            succeeded, failed, ..
        } => format!("StackApplyCompleted ({succeeded} ok, {failed} failed)"),
        StackEvent::StackApplyFailed { error, .. } => {
            format!("StackApplyFailed: {error}")
        }
        StackEvent::ServiceCreating { service_name, .. } => {
            format!("ServiceCreating  {service_name}")
        }
        StackEvent::ServiceReady { service_name, .. } => {
            format!("ServiceReady     {service_name}")
        }
        StackEvent::ServiceStopping { service_name, .. } => {
            format!("ServiceStopping  {service_name}")
        }
        StackEvent::ServiceStopped {
            service_name,
            exit_code,
            ..
        } => format!("ServiceStopped   {service_name} (exit {exit_code})"),
        StackEvent::ServiceFailed {
            service_name,
            error,
            ..
        } => format!("ServiceFailed    {service_name}: {error}"),
        StackEvent::PortConflict {
            service_name, port, ..
        } => format!("PortConflict     {service_name} port {port}"),
        StackEvent::VolumeCreated { volume_name, .. } => {
            format!("VolumeCreated    {volume_name}")
        }
        StackEvent::StackDestroyed { stack_name } => {
            format!("StackDestroyed   {stack_name}")
        }
        StackEvent::HealthCheckPassed { service_name, .. } => {
            format!("HealthCheckOk    {service_name}")
        }
        StackEvent::HealthCheckFailed {
            service_name,
            attempt,
            error,
            ..
        } => format!("HealthCheckFail  {service_name} #{attempt}: {error}"),
        StackEvent::DependencyBlocked {
            service_name,
            waiting_on,
            ..
        } => format!(
            "DependencyBlock  {service_name} -> {}",
            waiting_on.join(", ")
        ),
    }
}

/// Extract a short timestamp (HH:MM:SS) from an ISO 8601 datetime string.
fn short_timestamp(created_at: &str) -> &str {
    // ISO 8601: "2024-01-15 14:22:15" or "2024-01-15T14:22:15"
    // We want just the time part.
    if created_at.len() >= 19 {
        &created_at[11..19]
    } else {
        created_at
    }
}

// ── Terminal guard ─────────────────────────────────────────────────

/// RAII guard that restores terminal state on drop (including panics).
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

// ── Entry point ────────────────────────────────────────────────────

/// Returns `true` if stdout is an interactive terminal.
pub fn is_tty() -> bool {
    io::stdout().is_terminal()
}

/// Run the TUI event loop.
///
/// This takes over the terminal and presents a dashboard that polls
/// the state store at `db_path` every 500ms. Returns when the user
/// presses `q` or `Ctrl-C`.
pub fn run_tui(stack_name: String, spec: StackSpec, db_path: PathBuf) -> anyhow::Result<()> {
    enable_raw_mode().context("failed to enable raw terminal mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let terminal =
        Terminal::new(backend).context("failed to initialize terminal for TUI dashboard")?;

    let mut guard = TerminalGuard { terminal };

    let store =
        StateStore::open(&db_path).context("failed to open state store for TUI dashboard")?;
    let mut app = App::new(stack_name, spec, store);

    // Initial data load.
    app.refresh_data();

    let tick_rate = Duration::from_millis(500);
    let mut last_tick = Instant::now();

    loop {
        guard
            .terminal
            .draw(|frame| ui(frame, &mut app))
            .context("failed to draw TUI frame")?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout).context("failed to poll terminal events")? {
            if let Event::Key(key) = event::read().context("failed to read terminal event")? {
                app.handle_key(key);
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.refresh_data();
            last_tick = Instant::now();
        }

        if app.should_quit {
            break;
        }
    }

    // TerminalGuard::drop handles cleanup.
    Ok(())
}

// ── Rendering ──────────────────────────────────────────────────────

/// Root rendering function.
fn ui(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header + tabs
            Constraint::Min(10),   // main content
            Constraint::Length(7), // bottom event strip
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);

    match app.active_tab {
        Tab::Services => render_services(frame, chunks[1], app),
        Tab::Events => render_events_tab(frame, chunks[1], app),
        Tab::Logs => render_logs(frame, chunks[1], app),
    }

    render_event_strip(frame, chunks[2], app);

    if app.show_help {
        render_help_overlay(frame);
    }
}

/// Render the header bar with stack name, tabs, and keybinding hints.
fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let titles: Vec<Line> = Tab::ALL
        .iter()
        .map(|t| {
            let style = if *t == app.active_tab {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Line::from(Span::styled(t.label(), style))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" vz stack: {} ", app.stack_name))
                .title_alignment(Alignment::Left),
        )
        .highlight_style(Style::default().fg(Color::Yellow))
        .divider(Span::raw(" | "));

    frame.render_widget(tabs, area);

    // Render keybinding hints on the right side of the header.
    let hints = Span::styled("q:quit ?:help ", Style::default().fg(Color::DarkGray));
    let hints_width = hints.width() as u16;
    if area.width > hints_width + 2 {
        let hints_area = Rect {
            x: area.x + area.width - hints_width - 1,
            y: area.y,
            width: hints_width,
            height: 1,
        };
        frame.render_widget(Paragraph::new(hints), hints_area);
    }
}

/// Render the Services tab: table + detail bar.
fn render_services(frame: &mut Frame, area: Rect, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // service table
            Constraint::Length(3), // detail bar
        ])
        .split(area);

    render_service_table(frame, chunks[0], app);
    render_detail_bar(frame, chunks[1], app);
}

/// Render the service table with status and health columns.
fn render_service_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let header_cells = ["SERVICE", "IMAGE", "STATUS", "HEALTH", "PORTS"]
        .iter()
        .map(|h| {
            Cell::from(*h).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
        });
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = app
        .services
        .iter()
        .enumerate()
        .map(|(i, svc)| {
            // Find the corresponding spec for image and ports.
            let spec_svc = app
                .spec
                .services
                .iter()
                .find(|s| s.name == svc.service_name);
            let image = spec_svc.map_or("-", |s| s.image.as_str());
            let ports = spec_svc
                .map(|s| {
                    s.ports
                        .iter()
                        .map(|p| {
                            if let Some(hp) = p.host_port {
                                format!("{}:{}", hp, p.container_port)
                            } else {
                                format!("{}", p.container_port)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let ports_display = if ports.is_empty() {
                "-".to_string()
            } else {
                ports
            };

            let phase_str = format!("{:?}", svc.phase);
            let color = phase_color(&svc.phase);

            let health = if svc.ready {
                Cell::from("ok").style(Style::default().fg(Color::Green))
            } else if svc.phase == ServicePhase::Failed {
                Cell::from("fail").style(Style::default().fg(Color::Red))
            } else {
                Cell::from("-").style(Style::default().fg(Color::DarkGray))
            };

            let prefix = if i == app.selected_service {
                "> "
            } else {
                "  "
            };

            Row::new(vec![
                Cell::from(format!("{prefix}{}", svc.service_name)),
                Cell::from(image.to_string()),
                Cell::from(phase_str).style(Style::default().fg(color)),
                health,
                Cell::from(ports_display),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Min(20),
        Constraint::Length(12),
        Constraint::Length(8),
        Constraint::Min(14),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Services "))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    let mut state = TableState::default();
    if !app.services.is_empty() {
        state.select(Some(app.selected_service));
    }

    frame.render_stateful_widget(table, area, &mut state);
}

/// Render the detail bar showing selected service info.
fn render_detail_bar(frame: &mut Frame, area: Rect, app: &App) {
    let detail = if let Some(svc) = app.services.get(app.selected_service) {
        let cid = svc.container_id.as_deref().unwrap_or("-");
        let spec_svc = app
            .spec
            .services
            .iter()
            .find(|s| s.name == svc.service_name);
        let deps = spec_svc
            .map(|s| {
                s.depends_on
                    .iter()
                    .map(|d| d.service.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let deps_display = if deps.is_empty() {
            "none".to_string()
        } else {
            deps
        };

        let error_display = svc
            .last_error
            .as_deref()
            .map(|e| format!(" | error: {e}"))
            .unwrap_or_default();

        format!(
            "{} | {} | depends: {}{}",
            svc.service_name, cid, deps_display, error_display
        )
    } else {
        "No service selected".to_string()
    };

    let paragraph = Paragraph::new(detail)
        .block(Block::default().borders(Borders::ALL).title(" Detail "))
        .style(Style::default().fg(Color::White));

    frame.render_widget(paragraph, area);
}

/// Render the Events tab: scrollable event list.
fn render_events_tab(frame: &mut Frame, area: Rect, app: &mut App) {
    let items: Vec<ListItem> = app
        .events
        .iter()
        .map(|record| {
            let time = short_timestamp(&record.created_at);
            let summary = format_event_summary(&record.event);
            let color = event_color(&record.event);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{time}  "), Style::default().fg(Color::DarkGray)),
                Span::styled(summary, Style::default().fg(color)),
            ]))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Events "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    let mut state = ListState::default();
    if !app.events.is_empty() {
        state.select(Some(app.event_scroll));
    }

    frame.render_stateful_widget(list, area, &mut state);
}

/// Render the Logs tab: service sidebar + log content.
fn render_logs(frame: &mut Frame, area: Rect, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(20), // service sidebar
            Constraint::Min(30),    // log content
        ])
        .split(area);

    // Service sidebar.
    let items: Vec<ListItem> = app
        .spec
        .services
        .iter()
        .map(|s| {
            let phase = app
                .services
                .iter()
                .find(|o| o.service_name == s.name)
                .map(|o| &o.phase);
            let color = phase.map_or(Color::DarkGray, phase_color);
            ListItem::new(Span::styled(&s.name, Style::default().fg(color)))
        })
        .collect();

    let sidebar = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Services "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    let mut sidebar_state = ListState::default();
    if !app.spec.services.is_empty() {
        sidebar_state.select(Some(app.selected_log_service));
    }

    frame.render_stateful_widget(sidebar, chunks[0], &mut sidebar_state);

    // Log content.
    let service_name = app
        .spec
        .services
        .get(app.selected_log_service)
        .map(|s| s.name.as_str())
        .unwrap_or("");

    let log_content = app.logs.get(service_name).cloned().unwrap_or_default();

    let display = if log_content.is_empty() {
        "Logs not available (service not running or no output yet)".to_string()
    } else {
        log_content
    };

    let paragraph = Paragraph::new(display)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Logs: {service_name} ")),
        )
        .scroll((app.log_scroll as u16, 0))
        .wrap(Wrap { trim: false });

    frame.render_widget(paragraph, chunks[1]);
}

/// Render the bottom event strip (last 5 events, always visible).
fn render_event_strip(frame: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .recent_events
        .iter()
        .map(|record| {
            let time = short_timestamp(&record.created_at);
            let summary = format_event_summary(&record.event);
            let color = event_color(&record.event);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{time}  "), Style::default().fg(Color::DarkGray)),
                Span::styled(summary, Style::default().fg(color)),
            ]))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Recent Events "),
    );

    frame.render_widget(list, area);
}

/// Render a centered help overlay.
fn render_help_overlay(frame: &mut Frame) {
    let area = frame.area();
    let help_width = 50u16;
    let help_height = 16u16;

    let x = area.width.saturating_sub(help_width) / 2;
    let y = area.height.saturating_sub(help_height) / 2;

    let help_area = Rect {
        x,
        y,
        width: help_width.min(area.width),
        height: help_height.min(area.height),
    };

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Keybindings",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("  q / Ctrl-C    Quit"),
        Line::from("  Tab           Next tab"),
        Line::from("  1 / 2 / 3    Switch to tab"),
        Line::from("  j / Down      Navigate down"),
        Line::from("  k / Up        Navigate up"),
        Line::from("  g             Jump to top"),
        Line::from("  G             Jump to bottom"),
        Line::from("  ?             Toggle this help"),
        Line::from(""),
        Line::from(Span::styled(
            "  Press any key to dismiss",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    // Clear the background.
    let clear = Clear;
    frame.render_widget(clear, help_area);

    let paragraph = Paragraph::new(help_text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Help ")
            .border_style(Style::default().fg(Color::Yellow)),
    );

    frame.render_widget(paragraph, help_area);
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn phase_color_mapping() {
        assert_eq!(phase_color(&ServicePhase::Running), Color::Green);
        assert_eq!(phase_color(&ServicePhase::Failed), Color::Red);
        assert_eq!(phase_color(&ServicePhase::Pending), Color::Yellow);
        assert_eq!(phase_color(&ServicePhase::Creating), Color::Cyan);
        assert_eq!(phase_color(&ServicePhase::Stopping), Color::DarkGray);
        assert_eq!(phase_color(&ServicePhase::Stopped), Color::DarkGray);
    }

    #[test]
    fn event_color_mapping() {
        assert_eq!(
            event_color(&StackEvent::StackApplyStarted {
                stack_name: "s".into(),
                services_count: 1,
            }),
            Color::Blue
        );
        assert_eq!(
            event_color(&StackEvent::StackApplyCompleted {
                stack_name: "s".into(),
                succeeded: 1,
                failed: 0,
            }),
            Color::Green
        );
        assert_eq!(
            event_color(&StackEvent::StackApplyFailed {
                stack_name: "s".into(),
                error: "e".into(),
            }),
            Color::Red
        );
        assert_eq!(
            event_color(&StackEvent::ServiceCreating {
                stack_name: "s".into(),
                service_name: "w".into(),
            }),
            Color::Cyan
        );
        assert_eq!(
            event_color(&StackEvent::ServiceReady {
                stack_name: "s".into(),
                service_name: "w".into(),
                runtime_id: "r".into(),
            }),
            Color::Green
        );
        assert_eq!(
            event_color(&StackEvent::ServiceStopping {
                stack_name: "s".into(),
                service_name: "w".into(),
            }),
            Color::DarkGray
        );
        assert_eq!(
            event_color(&StackEvent::ServiceStopped {
                stack_name: "s".into(),
                service_name: "w".into(),
                exit_code: 0,
            }),
            Color::DarkGray
        );
        assert_eq!(
            event_color(&StackEvent::ServiceFailed {
                stack_name: "s".into(),
                service_name: "w".into(),
                error: "oom".into(),
            }),
            Color::Red
        );
        assert_eq!(
            event_color(&StackEvent::PortConflict {
                stack_name: "s".into(),
                service_name: "w".into(),
                port: 80,
            }),
            Color::Red
        );
        assert_eq!(
            event_color(&StackEvent::VolumeCreated {
                stack_name: "s".into(),
                volume_name: "v".into(),
            }),
            Color::Blue
        );
        assert_eq!(
            event_color(&StackEvent::StackDestroyed {
                stack_name: "s".into(),
            }),
            Color::Yellow
        );
        assert_eq!(
            event_color(&StackEvent::HealthCheckPassed {
                stack_name: "s".into(),
                service_name: "w".into(),
            }),
            Color::Green
        );
        assert_eq!(
            event_color(&StackEvent::HealthCheckFailed {
                stack_name: "s".into(),
                service_name: "w".into(),
                attempt: 1,
                error: "t".into(),
            }),
            Color::Yellow
        );
        assert_eq!(
            event_color(&StackEvent::DependencyBlocked {
                stack_name: "s".into(),
                service_name: "w".into(),
                waiting_on: vec!["db".into()],
            }),
            Color::Magenta
        );
    }

    #[test]
    fn event_summary_covers_all_variants() {
        let events = vec![
            StackEvent::StackApplyStarted {
                stack_name: "s".into(),
                services_count: 2,
            },
            StackEvent::StackApplyCompleted {
                stack_name: "s".into(),
                succeeded: 1,
                failed: 0,
            },
            StackEvent::StackApplyFailed {
                stack_name: "s".into(),
                error: "e".into(),
            },
            StackEvent::ServiceCreating {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceReady {
                stack_name: "s".into(),
                service_name: "web".into(),
                runtime_id: "ctr-1".into(),
            },
            StackEvent::ServiceStopping {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::ServiceStopped {
                stack_name: "s".into(),
                service_name: "web".into(),
                exit_code: 0,
            },
            StackEvent::ServiceFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                error: "oom".into(),
            },
            StackEvent::PortConflict {
                stack_name: "s".into(),
                service_name: "web".into(),
                port: 80,
            },
            StackEvent::VolumeCreated {
                stack_name: "s".into(),
                volume_name: "v".into(),
            },
            StackEvent::StackDestroyed {
                stack_name: "s".into(),
            },
            StackEvent::HealthCheckPassed {
                stack_name: "s".into(),
                service_name: "web".into(),
            },
            StackEvent::HealthCheckFailed {
                stack_name: "s".into(),
                service_name: "web".into(),
                attempt: 3,
                error: "timeout".into(),
            },
            StackEvent::DependencyBlocked {
                stack_name: "s".into(),
                service_name: "web".into(),
                waiting_on: vec!["db".into()],
            },
        ];

        for event in events {
            let summary = format_event_summary(&event);
            assert!(!summary.is_empty(), "empty summary for {event:?}");
        }
    }

    #[test]
    fn short_timestamp_extraction() {
        assert_eq!(short_timestamp("2024-01-15 14:22:15"), "14:22:15");
        assert_eq!(short_timestamp("2024-01-15T14:22:15"), "14:22:15");
        // Short strings returned as-is.
        assert_eq!(short_timestamp("14:22"), "14:22");
    }

    #[test]
    fn tab_next_cycles() {
        assert_eq!(Tab::Services.next(), Tab::Events);
        assert_eq!(Tab::Events.next(), Tab::Logs);
        assert_eq!(Tab::Logs.next(), Tab::Services);
    }

    #[test]
    fn tab_labels() {
        assert_eq!(Tab::Services.label(), "Services");
        assert_eq!(Tab::Events.label(), "Events");
        assert_eq!(Tab::Logs.label(), "Logs");
    }

    fn make_test_spec() -> StackSpec {
        StackSpec {
            name: "test".into(),
            services: vec![
                vz_stack::ServiceSpec {
                    name: "web".into(),
                    image: "nginx:latest".into(),
                    command: None,
                    entrypoint: None,
                    environment: std::collections::HashMap::new(),
                    working_dir: None,
                    user: None,
                    mounts: vec![],
                    ports: vec![vz_stack::PortSpec {
                        protocol: "tcp".into(),
                        container_port: 80,
                        host_port: Some(8080),
                    }],
                    depends_on: vec![vz_stack::ServiceDependency::started("db")],
                    healthcheck: None,
                    restart_policy: None,
                    resources: Default::default(),
                    extra_hosts: vec![],
                    secrets: vec![],
                    networks: vec![],
                    cap_add: vec![],
                    cap_drop: vec![],
                    privileged: false,
                    read_only: false,
                    sysctls: std::collections::HashMap::new(),
                    ulimits: vec![],
                    hostname: None,
                    domainname: None,
                    labels: std::collections::HashMap::new(),
                    stop_signal: None,
                    stop_grace_period_secs: None,
                    container_name: None,
                },
                vz_stack::ServiceSpec {
                    name: "db".into(),
                    image: "postgres:16".into(),
                    command: None,
                    entrypoint: None,
                    environment: std::collections::HashMap::new(),
                    working_dir: None,
                    user: None,
                    mounts: vec![],
                    ports: vec![vz_stack::PortSpec {
                        protocol: "tcp".into(),
                        container_port: 5432,
                        host_port: Some(5432),
                    }],
                    depends_on: vec![],
                    healthcheck: None,
                    restart_policy: None,
                    resources: Default::default(),
                    extra_hosts: vec![],
                    secrets: vec![],
                    networks: vec![],
                    cap_add: vec![],
                    cap_drop: vec![],
                    privileged: false,
                    read_only: false,
                    sysctls: std::collections::HashMap::new(),
                    ulimits: vec![],
                    hostname: None,
                    domainname: None,
                    labels: std::collections::HashMap::new(),
                    stop_signal: None,
                    stop_grace_period_secs: None,
                    container_name: None,
                },
            ],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
        }
    }

    #[test]
    fn app_new_initializes_correctly() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let app = App::new("test".into(), spec, store);

        assert_eq!(app.stack_name, "test");
        assert_eq!(app.active_tab, Tab::Services);
        assert_eq!(app.selected_service, 0);
        assert!(!app.should_quit);
        assert!(!app.show_help);
        assert!(app.logs.contains_key("web"));
        assert!(app.logs.contains_key("db"));
    }

    #[test]
    fn handle_key_quit() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));
        assert!(app.should_quit);
    }

    #[test]
    fn handle_key_ctrl_c() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        app.handle_key(event::KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        ));
        assert!(app.should_quit);
    }

    #[test]
    fn handle_key_tab_switch() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        assert_eq!(app.active_tab, Tab::Services);

        app.handle_key(event::KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Events);

        app.handle_key(event::KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Logs);

        app.handle_key(event::KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Services);
    }

    #[test]
    fn handle_key_number_tabs() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('2'), KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Events);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('3'), KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Logs);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE));
        assert_eq!(app.active_tab, Tab::Services);
    }

    #[test]
    fn handle_key_help_toggle() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        assert!(app.show_help);

        // Any key dismisses help.
        app.handle_key(event::KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        assert!(!app.show_help);
    }

    #[test]
    fn navigate_services() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        // Add some observed services.
        app.services = vec![
            ServiceObservedState {
                service_name: "web".into(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-1".into()),
                last_error: None,
                ready: true,
            },
            ServiceObservedState {
                service_name: "db".into(),
                phase: ServicePhase::Running,
                container_id: Some("ctr-2".into()),
                last_error: None,
                ready: true,
            },
        ];

        assert_eq!(app.selected_service, 0);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        assert_eq!(app.selected_service, 1);

        // Should not go past the end.
        app.handle_key(event::KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        assert_eq!(app.selected_service, 1);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE));
        assert_eq!(app.selected_service, 0);

        // Should not go below 0.
        app.handle_key(event::KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE));
        assert_eq!(app.selected_service, 0);
    }

    #[test]
    fn navigate_jump_top_bottom() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();
        let mut app = App::new("test".into(), spec, store);

        app.services = vec![
            ServiceObservedState {
                service_name: "web".into(),
                phase: ServicePhase::Running,
                container_id: None,
                last_error: None,
                ready: true,
            },
            ServiceObservedState {
                service_name: "db".into(),
                phase: ServicePhase::Running,
                container_id: None,
                last_error: None,
                ready: true,
            },
        ];

        app.handle_key(event::KeyEvent::new(
            KeyCode::Char('G'),
            KeyModifiers::SHIFT,
        ));
        assert_eq!(app.selected_service, 1);

        app.handle_key(event::KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE));
        assert_eq!(app.selected_service, 0);
    }

    #[test]
    fn refresh_data_loads_from_store() {
        let spec = make_test_spec();
        let store = StateStore::in_memory().unwrap();

        // Seed some data.
        store
            .save_observed_state(
                "test",
                &ServiceObservedState {
                    service_name: "web".into(),
                    phase: ServicePhase::Running,
                    container_id: Some("ctr-abc".into()),
                    last_error: None,
                    ready: true,
                },
            )
            .unwrap();

        store
            .emit_event(
                "test",
                &StackEvent::ServiceReady {
                    stack_name: "test".into(),
                    service_name: "web".into(),
                    runtime_id: "ctr-abc".into(),
                },
            )
            .unwrap();

        let mut app = App::new("test".into(), spec, store);
        app.refresh_data();

        assert_eq!(app.services.len(), 1);
        assert_eq!(app.services[0].service_name, "web");
        assert!(!app.events.is_empty());
        assert!(!app.recent_events.is_empty());
    }

    #[test]
    fn is_tty_returns_bool() {
        // In test environment, stdout is typically not a TTY.
        // Just verify it doesn't panic and returns a bool.
        let _ = is_tty();
    }
}
