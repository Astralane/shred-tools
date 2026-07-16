//! Opt-in live terminal dashboard (`--tui`).
//!
//! Shows the head-to-head provider comparison only — winrate, mean microseconds
//! behind the fastest, and coverage — and never invalid / bad-signature counts. A
//! live glance answers "who is fastest"; whether a provider *tampered* is a
//! deliberate, offline judgement made against the archive, not a number that
//! flickers past on a dashboard.

use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use ratatui::{
    backend::CrosstermBackend,
    crossterm::{
        event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    },
    layout::{Constraint, Layout},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Row, Table},
    Frame, Terminal,
};

use crate::{live::LiveStats, registry::Registry};

pub struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Tui {
    /// Take over the terminal. Also installs a panic hook that restores it, since
    /// this build aborts on panic (no unwinding, so `Drop` would not run).
    pub fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
            prev(info);
        }));

        let terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
        Ok(Self { terminal })
    }

    /// Non-blocking check for a quit request: `q`, `Esc`, or `Ctrl-C` (in raw mode
    /// `Ctrl-C` is a key event, not a signal, so the ctrlc handler never sees it).
    pub fn quit_requested(&self) -> Result<bool> {
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(k) = event::read()? {
                if k.kind == KeyEventKind::Press {
                    let ctrl_c = k.code == KeyCode::Char('c')
                        && k.modifiers.contains(KeyModifiers::CONTROL);
                    if ctrl_c || matches!(k.code, KeyCode::Char('q') | KeyCode::Esc) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn draw(&mut self, stats: &LiveStats, reg: &Registry, footer: &str) -> Result<()> {
        self.terminal.draw(|f| render(f, stats, reg, footer))?;
        Ok(())
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn render(f: &mut Frame, stats: &LiveStats, reg: &Registry, footer: &str) {
    let areas = Layout::vertical([
        Constraint::Length(1), // title
        Constraint::Min(3),    // comparison table
        Constraint::Length(1), // footer
    ])
    .split(f.area());

    let total = stats.total_sets();

    // Rank providers by winrate (then coverage), so the fastest is on top.
    let mut ids: Vec<u16> = (0..reg.len() as u16).collect();
    ids.sort_by(|&a, &b| {
        let (pa, pb) = (stats.provider(a), stats.provider(b));
        let wb = pb.winrate().unwrap_or(-1.0);
        let wa = pa.winrate().unwrap_or(-1.0);
        wb.partial_cmp(&wa)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                let cb = pb.coverage(total).unwrap_or(0.0);
                let ca = pa.coverage(total).unwrap_or(0.0);
                cb.partial_cmp(&ca).unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let header = Row::new(["provider", "winrate", "µs behind", "coverage", "valid/seen"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let rows = ids.iter().map(|&id| {
        let p = stats.provider(id);
        Row::new([
            reg.name(id).to_string(),
            p.winrate()
                .map(|w| format!("{:.1}%", w * 100.0))
                .unwrap_or_else(|| "—".into()),
            p.mean_behind_us()
                .map(|u| format!("{u:.1}"))
                .unwrap_or_else(|| "—".into()),
            p.coverage(total)
                .map(|c| format!("{:.1}%", c * 100.0))
                .unwrap_or_else(|| "—".into()),
            format!("{}/{}", fmt_int(p.valid), fmt_int(p.present)),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Min(12),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(9),
            Constraint::Length(16),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" shred-audit — live provider comparison "),
    );

    let title = Line::from(vec![
        Span::styled("shred-audit", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "   {} sets · {} contested",
            fmt_int(total),
            fmt_int(stats.contested_sets())
        )),
    ]);

    f.render_widget(Paragraph::new(title), areas[0]);
    f.render_widget(table, areas[1]);

    let foot = if footer.is_empty() {
        "q / Esc to quit".to_string()
    } else {
        format!("{footer}    ·    q / Esc to quit")
    };
    f.render_widget(
        Paragraph::new(Line::from(foot)).style(Style::default().add_modifier(Modifier::DIM)),
        areas[2],
    );
}

/// Group a number with thin thousands separators for readability.
fn fmt_int(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}
