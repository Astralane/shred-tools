//! Live dashboard. Deliberately omits per-shred invalid/bad-signature counts: judging
//! tampering is an offline call against the archive, not a flickering number.

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

use crate::{
    live::LiveStats,
    out::{TxnCompareSummary, TxnSource},
    registry::Registry,
};

pub struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
}

impl Tui {
    pub fn enter() -> Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen)?;

        // Release builds abort on panic, so `Drop` would never restore the terminal.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            prev(info);
        }));

        let terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
        Ok(Self { terminal })
    }

    /// In raw mode Ctrl-C is a key event, not a signal, so it is handled here.
    pub fn quit_requested(&self) -> Result<bool> {
        if !event::poll(Duration::ZERO)? {
            return Ok(false);
        }
        let Event::Key(k) = event::read()? else {
            return Ok(false);
        };
        let ctrl_c = k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL);
        Ok(k.kind == KeyEventKind::Press
            && (ctrl_c || matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)))
    }

    pub fn draw(
        &mut self,
        stats: &LiveStats,
        reg: &Registry,
        txn: Option<&TxnCompareSummary>,
        footer: &str,
    ) -> Result<()> {
        self.terminal.draw(|f| render(f, stats, reg, txn, footer))?;
        Ok(())
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn bold() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

fn or_dash(v: Option<f64>, fmt: impl Fn(f64) -> String) -> String {
    v.map(fmt).unwrap_or_else(|| "—".into())
}

fn pct(v: Option<f64>) -> String {
    or_dash(v, |x| format!("{:.1}%", x * 100.0))
}

fn us(v: Option<f64>) -> String {
    or_dash(v, |x| format!("{x:.1}"))
}

fn render(
    f: &mut Frame,
    stats: &LiveStats,
    reg: &Registry,
    txn: Option<&TxnCompareSummary>,
    footer: &str,
) {
    let mut txn_srcs: Vec<&TxnSource> = txn
        .map(|t| t.sources.iter().filter(|s| s.seen > 0).collect())
        .unwrap_or_default();

    let mut constraints = vec![Constraint::Length(1), Constraint::Min(3)];
    if !txn_srcs.is_empty() {
        constraints.push(Constraint::Length(txn_srcs.len() as u16 + 3));
    }
    constraints.push(Constraint::Length(1));
    let areas = Layout::vertical(constraints).split(f.area());

    let total = stats.total_sets();
    let title = Line::from(vec![
        Span::styled("shred-audit", bold()),
        Span::raw(format!(
            "   {} sets · {} contested",
            fmt_int(total),
            fmt_int(stats.contested_sets())
        )),
    ]);
    f.render_widget(Paragraph::new(title), areas[0]);

    // Fastest on top: by winrate, then coverage.
    let mut ids: Vec<u16> = (0..reg.len() as u16).collect();
    ids.sort_by(|&a, &b| {
        let (pa, pb) = (stats.provider(a), stats.provider(b));
        let wa = pa.winrate().unwrap_or(-1.0);
        let wb = pb.winrate().unwrap_or(-1.0);
        let ca = pa.coverage(total).unwrap_or(0.0);
        let cb = pb.coverage(total).unwrap_or(0.0);
        wb.total_cmp(&wa).then(cb.total_cmp(&ca))
    });
    let rows = ids.iter().map(|&id| {
        let p = stats.provider(id);
        Row::new([
            reg.name(id).to_string(),
            pct(p.winrate()),
            us(p.mean_behind_us()),
            pct(p.coverage(total)),
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
    .header(
        Row::new(["provider", "winrate", "µs behind", "coverage", "valid/seen"]).style(bold()),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" shred-audit — live provider comparison "),
    );
    f.render_widget(table, areas[1]);

    if let Some(t) = txn.filter(|_| !txn_srcs.is_empty()) {
        txn_srcs.sort_by(|a, b| b.winrate.unwrap_or(-1.0).total_cmp(&a.winrate.unwrap_or(-1.0)));
        let rows = txn_srcs.into_iter().map(|s| {
            Row::new([
                s.name.clone(),
                s.kind.label().to_string(),
                pct(s.winrate),
                us(s.behind_p50_us),
                us(s.behind_p90_us),
                fmt_int(s.seen),
                fmt_bad_sigs(s),
            ])
        });
        let audited = if t.onchain_slots_checked > 0 {
            format!(" · {} slots audited onchain", fmt_int(t.onchain_slots_checked))
        } else {
            String::new()
        };
        let table = Table::new(
            rows,
            [
                Constraint::Min(14),
                Constraint::Length(13),
                Constraint::Length(9),
                Constraint::Length(11),
                Constraint::Length(9),
                Constraint::Length(12),
                Constraint::Length(17),
            ],
        )
        .header(
            Row::new(["source", "kind", "winrate", "µs behind", "µs p90", "seen", "bad sigs"])
                .style(bold()),
        )
        .block(Block::default().borders(Borders::ALL).title(format!(
            " transaction race — shreds vs gRPC · {} contested txns{audited} ",
            fmt_int(t.contested)
        )));
        f.render_widget(table, areas[2]);
    }

    let foot = if footer.is_empty() {
        "q / Esc to quit".to_string()
    } else {
        format!("{footer}    ·    q / Esc to quit")
    };
    f.render_widget(
        Paragraph::new(Line::from(foot)).style(Style::default().add_modifier(Modifier::DIM)),
        areas[areas.len() - 1],
    );
}

/// Rate plus raw count: early in a capture the rate may rest on a single slot.
fn fmt_bad_sigs(s: &TxnSource) -> String {
    // No rate must not render as 0%, which would read as clean.
    let Some(p) = s.onchain_bad_pct else {
        return "—".into();
    };
    let pct = p * 100.0;
    // Likewise a real but tiny discrepancy must not round to 0.00%.
    let pct = if s.onchain_bad > 0 && pct < 0.01 {
        "<0.01%".to_string()
    } else if pct >= 1.0 {
        format!("{pct:.1}%")
    } else {
        format!("{pct:.2}%")
    };
    format!("{pct} ({})", fmt_int(s.onchain_bad))
}

/// Thousands separators.
fn fmt_int(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}
