mod agg;
mod config;
mod deshred;
mod filters;
mod grpc;
mod leader;
mod live;
mod names;
mod out;
mod pinger;
mod registry;
mod rpc;
mod rx;
mod sigreg;
mod tui;
mod txncmp;
mod verification;
mod verify;
#[cfg(test)]
mod verify_realsig_test;

use std::{
    io::IsTerminal,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use crossbeam_channel::RecvTimeoutError;

use crate::{
    agg::{Aggregator, SetRow},
    config::{Config, ExportMode},
    leader::LeaderSchedule,
    live::LiveStats,
    out::{build_manifest, Sink, TxnCompareSummary, WindowStats, VERSION},
    registry::Registry,
    rx::RxStats,
    tui::Tui,
    txncmp::TxnCompare,
    verify::{verify_chunk, VerifiedShred, VerifyStats},
};

#[derive(Parser)]
#[command(name = "shred-audit", version)]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: String,
    #[arg(long, value_enum)]
    export: Option<ExportMode>,
    /// Also emit shreds.parquet, one row per shred (~1GB per 10 min at 100 kpps).
    #[arg(long)]
    dump_shreds: bool,
    #[arg(long)]
    dump_txns: bool,
    /// Stop after this many seconds. 0 = run until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// Print a periodic status line instead of the live TUI.
    /// Automatic when stdout is not a terminal.
    #[arg(long)]
    no_tui: bool,
    /// Also refresh `<output_dir>/live.zip` every `live_secs` (default 10)
    /// for the web viewer's watch-URL mode. Serve the dir yourself.
    #[arg(long)]
    live: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).context("loading config")?;
    let registry = Arc::new(Registry::build(&cfg));
    let export_mode = args.export.unwrap_or(cfg.export.mode);

    if export_mode != ExportMode::Zip && (args.dump_shreds || args.dump_txns || args.live) {
        bail!(
            "--dump-shreds, --dump-txns and --live all write into the zip archive, but export \
             mode is `{}`. Use `--export zip`, or drop the flag",
            export_mode.label()
        );
    }

    eprintln!(
        "shred-audit {VERSION}: {} providers, ports {:?}, {} verify threads, export {}",
        registry.len(),
        cfg.listen_ports,
        cfg.verify_thread_count(),
        export_mode.label(),
    );

    let rpc = cfg.rpc_endpoint()?;
    let schedule = LeaderSchedule::new(rpc.clone());
    schedule
        .refresh()
        .with_context(|| format!("initial leader schedule fetch via {}", rpc.label()))?;
    if let Err(e) = schedule.refresh_names() {
        eprintln!("validator names unavailable ({e:#}); the viewer will show pubkeys");
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.verify_thread_count())
        .thread_name(|i| format!("verify-{i}"))
        .build_global()
        .ok();

    let exit = Arc::new(AtomicBool::new(false));
    {
        let exit = exit.clone();
        let msg = if export_mode == ExportMode::Zip {
            "\nshutting down, flushing archive..."
        } else {
            "\nshutting down..."
        };
        let ctrlc_count = AtomicUsize::new(0);
        ctrlc::set_handler(move || {
            if ctrlc_count.fetch_add(1, Ordering::Relaxed) == 0 {
                eprintln!("{msg}");
                exit.store(true, Ordering::Relaxed);
            } else {
                eprintln!("\nforced exit");
                std::process::exit(130);
            }
        })?;
    }

    let rx_stats = Arc::new(RxStats::default());
    // Bounded so a verify backlog is dropped and counted, never reported as network latency.
    let (tx, rx_chan) = crossbeam_channel::bounded::<Vec<rx::Packet>>(16_384);
    let netmon = Arc::new(pinger::NetMon::default());

    let rx_handles = rx::spawn_receivers(
        cfg.bind_ip,
        &cfg.listen_ports,
        registry.clone(),
        netmon.clone(),
        tx.clone(),
        rx_stats.clone(),
        exit.clone(),
    )
    .context("binding sockets")?;
    // With no sockets (gRPC-only) nothing else holds a sender; keep the channel open.
    let keep_tx = cfg.listen_ports.is_empty().then_some(tx);

    pinger::spawn(netmon.clone(), cfg.clone(), exit.clone());

    let mut vstats = VerifyStats::default();
    let mut aggregator = Aggregator::new(cfg.fec_max_wait_slots);

    let filter_db = match export_mode {
        ExportMode::Postgres => Some(out::connection_url(&cfg.export.postgres)?),
        _ => None,
    };
    let mut txn_compare = TxnCompare::start(&cfg, args.dump_txns, filter_db)?;
    if args.dump_txns && txn_compare.is_none() {
        eprintln!(
            "--dump-txns has no effect: transactions.parquet comes from the transaction \
             comparison, and no `grpc_sources` are configured"
        );
    }
    let dump_txns = args.dump_txns && txn_compare.is_some();

    let mut sink = Sink::open(&cfg, export_mode, &registry, args.dump_shreds, dump_txns)?;
    let rotate_every = sink.rotate_interval();
    let flush_every = sink.flush_interval();
    let mut window_start = out::now_unix_ns();
    let mut stats = WindowStats::default();

    let started = Instant::now();
    let mut last_flush = Instant::now();
    let mut last_report = Instant::now();
    let mut mixed_warned = std::collections::HashSet::new();
    let mut last_sender_check = Instant::now();
    let mut last_txn = Instant::now();
    let mut last_rotate = Instant::now();
    let mut last_sched_check = Instant::now();
    let mut last_draw = Instant::now();
    let mut last_live = Instant::now();
    // Refreshed on a timer; recomputing it walks the whole signature set.
    let mut txn_snap: Option<TxnCompareSummary> = txn_compare.as_ref().map(|tc| tc.snapshot());
    let mut archives: Vec<PathBuf> = Vec::new();

    // Rows of the current window for live.zip; cleared on rotation.
    let live_secs = if cfg.live_secs == 0 { 10 } else { cfg.live_secs };
    let mut window_rows: Vec<SetRow> = Vec::new();

    if args.live {
        eprintln!(
            "live mode: refreshing {}/live.zip every {live_secs}s — serve that dir and point the \
             viewer's watch-URL at live.zip",
            cfg.output_dir
        );
        if cfg.rotate_secs == 0 {
            eprintln!(
                "  note: rotate_secs = 0, so the live window (and live.zip) grows for the whole \
                 run; set rotate_secs for a bounded live snapshot"
            );
        }
    }

    // Entered last so the startup messages above stay visible.
    let mut live = LiveStats::new();
    let mut footer = String::new();
    let mut tui = None;
    if !args.no_tui {
        if std::io::stdout().is_terminal() {
            match Tui::enter() {
                Ok(t) => tui = Some(t),
                Err(e) => {
                    eprintln!("could not start the TUI ({e:#}); using the periodic status line")
                }
            }
        } else {
            eprintln!(
                "stdout is not a terminal — showing the periodic status line instead of the TUI"
            );
        }
    }

    loop {
        let deadline_hit = args.duration_secs > 0
            && started.elapsed() >= Duration::from_secs(args.duration_secs);
        if exit.load(Ordering::Relaxed) || deadline_hit {
            break;
        }

        match rx_chan.recv_timeout(Duration::from_millis(100)) {
            Ok(packets) => {
                let verified = ingest(
                    packets,
                    txn_compare.as_ref(),
                    &schedule,
                    cfg.shred_version,
                    &mut vstats,
                    &mut aggregator,
                );
                if args.dump_shreds {
                    stats.rows_shreds += sink.write_shreds(&registry, &verified)?;
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        let rows = aggregator.harvest(false);
        if !rows.is_empty() {
            if tui.is_some() {
                live.ingest(&rows);
            }
            if args.live {
                window_rows.extend_from_slice(&rows);
            }
            stats.observe_sets(&rows);
            stats.rows_sets += sink.write_sets(&registry, &rows)?;
        }

        let needs_sched =
            aggregator.pending_sets() > 0 && schedule.needs_refresh(aggregator.max_slot());
        if needs_sched && last_sched_check.elapsed() >= Duration::from_secs(2) {
            last_sched_check = Instant::now();
            if let Err(e) = schedule.refresh() {
                let msg = format!("leader schedule refresh failed: {e:#}");
                notify(tui.is_some(), &mut footer, msg);
            }
        }

        if let Some(tc) = &txn_compare {
            if last_txn.elapsed() >= Duration::from_secs(2) {
                last_txn = Instant::now();
                txn_snap = Some(tc.snapshot());
                write_txns(tc, &mut sink, &mut stats)?;
            }
        }

        if let Some(t) = tui.as_mut() {
            if t.quit_requested()? {
                exit.store(true, Ordering::Relaxed);
            }
            if last_draw.elapsed() >= Duration::from_millis(400) {
                last_draw = Instant::now();
                t.draw(&live, &registry, txn_snap.as_ref(), &footer)?;
            }
        } else if last_report.elapsed() >= Duration::from_secs(10) {
            last_report = Instant::now();
            report(&rx_stats, &vstats, &aggregator, &stats, registry.names());
        }
        if last_sender_check.elapsed() >= Duration::from_secs(10) {
            last_sender_check = Instant::now();
            for warning in netmon.mixed_senders(&cfg, &registry) {
                if mixed_warned.insert(warning.clone()) {
                    notify(tui.is_some(), &mut footer, format!("warning: {warning}"));
                }
            }
        }

        if args.live && last_live.elapsed() >= Duration::from_secs(live_secs) {
            last_live = Instant::now();
            let manifest = build_manifest(
                &WindowStats::from_sets(&window_rows), &cfg, &registry, &netmon, &schedule,
                &rx_stats, &vstats, txn_snap.as_ref(), aggregator.shreds_after_window(),
                window_start,
            );
            if let Err(e) = sink.write_live(&registry, &window_rows, manifest) {
                notify(tui.is_some(), &mut footer, format!("live snapshot failed: {e:#}"));
            }
        }

        if flush_every.is_some_and(|every| last_flush.elapsed() >= every) {
            last_flush = Instant::now();
            let manifest = build_manifest(
                &stats, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
                txn_snap.as_ref(), aggregator.shreds_after_window(), window_start,
            );
            sink.flush(&manifest)?;
        }

        if rotate_every.is_some_and(|every| last_rotate.elapsed() >= every) {
            last_rotate = Instant::now();
            let rows = aggregator.harvest(true);
            if tui.is_some() {
                live.ingest(&rows);
            }
            stats.observe_sets(&rows);
            stats.rows_sets += sink.write_sets(&registry, &rows)?;
            if let Some(tc) = &txn_compare {
                write_txns(tc, &mut sink, &mut stats)?;
            }
            let manifest = build_manifest(
                &stats, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
                txn_snap.as_ref(), aggregator.shreds_after_window(), window_start,
            );
            if let Some(path) = sink.rotate(manifest)? {
                notify(tui.is_some(), &mut footer, format!("wrote {}", path.display()));
                archives.push(path);
            }

            window_start = out::now_unix_ns();
            stats = WindowStats::default();
            window_rows.clear();
        }
    }

    // Restore the terminal before any shutdown output goes to stderr.
    drop(tui);

    exit.store(true, Ordering::Relaxed);
    for h in rx_handles {
        let _ = h.join();
    }
    drop(keep_tx);
    while let Ok(packets) = rx_chan.recv() {
        let verified = ingest(
            packets,
            txn_compare.as_ref(),
            &schedule,
            cfg.shred_version,
            &mut vstats,
            &mut aggregator,
        );
        if args.dump_shreds {
            stats.rows_shreds += sink.write_shreds(&registry, &verified)?;
        }
    }
    let rows = aggregator.harvest(true);
    stats.observe_sets(&rows);
    stats.rows_sets += sink.write_sets(&registry, &rows)?;
    if let Some(tc) = txn_compare.as_mut() {
        tc.shutdown();
        txn_snap = Some(tc.final_snapshot());
        write_txns(tc, &mut sink, &mut stats)?;
    }
    // Refresh live.zip once more so it reflects the final state.
    if args.live {
        window_rows.extend_from_slice(&rows);
        let manifest = build_manifest(
            &WindowStats::from_sets(&window_rows), &cfg, &registry, &netmon, &schedule, &rx_stats,
            &vstats, txn_snap.as_ref(), aggregator.shreds_after_window(), window_start,
        );
        if let Err(e) = sink.write_live(&registry, &window_rows, manifest) {
            eprintln!("final live snapshot failed: {e:#}");
        }
    }
    let manifest = build_manifest(
        &stats, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats, txn_snap.as_ref(),
        aggregator.shreds_after_window(), window_start,
    );
    if let Some(path) = sink.close(manifest)? {
        eprintln!("wrote {}", path.display());
        archives.push(path);
    }

    report(&rx_stats, &vstats, &aggregator, &stats, registry.names());

    if let Some(tc) = txn_compare {
        tc.finish();
    }

    if archives.is_empty() {
        eprintln!("\nno archives written (export mode `{}`)", export_mode.label());
    } else {
        eprintln!("\n{} archive(s):", archives.len());
        for a in &archives {
            eprintln!("  {}", a.display());
        }
    }
    Ok(())
}

fn ingest(
    packets: Vec<rx::Packet>,
    txn_compare: Option<&TxnCompare>,
    schedule: &LeaderSchedule,
    shred_version: Option<u16>,
    vstats: &mut VerifyStats,
    aggregator: &mut Aggregator,
) -> Vec<VerifiedShred> {
    let verified = verify_chunk(&packets, schedule, shred_version, vstats);
    if let Some(tc) = txn_compare {
        for shred in verified.iter().filter(|shred| shred.is_authentic()) {
            let packet = &packets[shred.packet_index];
            tc.feed(packet.rx_unix_ns, packet.provider, &packet.data);
        }
    }
    for s in &verified {
        aggregator.ingest(s);
    }
    verified
}

/// With the TUI up, messages go to its footer instead of stderr.
fn notify(tui_active: bool, footer: &mut String, msg: String) {
    if tui_active {
        *footer = msg;
    } else {
        eprintln!("{msg}");
    }
}

fn write_txns(tc: &TxnCompare, sink: &mut Sink, stats: &mut WindowStats) -> Result<()> {
    let txns = tc.harvest();
    if !txns.is_empty() {
        stats.rows_txns += sink.write_txns(tc.labels(), &txns)?;
    }
    Ok(())
}

fn report(
    rx_stats: &RxStats,
    v: &VerifyStats,
    agg: &Aggregator,
    stats: &WindowStats,
    providers: &[String],
) {
    eprintln!(
        "rx {} (unmatched {}, no_ts {}, dropped {}, kernel_drop {}, trunc {}) | parsed {} bad_sig {} (data {}) proof_stripped {} no_leader {} malformed {} ping {} unsupported {} | ed25519 {} (batch_fallback {}) | after_window {} | pending sets {}",
        rx_stats.received.load(Ordering::Relaxed),
        rx_stats.unmatched.load(Ordering::Relaxed),
        rx_stats.no_timestamp.load(Ordering::Relaxed),
        rx_stats.channel_full.load(Ordering::Relaxed),
        rx_stats.kernel_dropped.load(Ordering::Relaxed),
        rx_stats.truncated.load(Ordering::Relaxed),
        v.parsed,
        v.sig_bad,
        stats.invalid_data,
        v.proof_stripped,
        v.no_leader,
        v.malformed,
        v.non_shred_ping,
        v.unsupported_variant,
        v.ed25519_verifies,
        v.batch_fallbacks,
        agg.shreds_after_window(),
        agg.pending_sets(),
    );
    for (id, name) in providers.iter().enumerate() {
        let p = v.providers.get(id).copied().unwrap_or_default();
        let invalid = stats.providers.get(id).copied().unwrap_or_default();
        eprintln!(
            "  {name}: parsed {} malformed {} wrong_version {} unsupported {} no_merkle_root {} no_leader {} bad_sig {} proof_stripped {} | invalid sig {} data {} unknown {}",
            p.parsed,
            p.malformed,
            p.wrong_version,
            p.unsupported_variant,
            p.no_merkle_root,
            p.no_leader,
            p.sig_bad,
            p.proof_stripped,
            invalid.invalid_sig,
            invalid.invalid_data,
            invalid.invalid_unknown,
        );
    }
}
