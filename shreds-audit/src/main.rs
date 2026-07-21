mod agg;
mod config;
mod deshred;
mod grpc;
mod leader;
mod live;
mod names;
mod out;
mod pinger;
mod proto;
mod registry;
mod rx;
mod sigreg;
mod tui;
mod txncmp;
mod verify;
#[cfg(test)]
mod verify_realsig_test;

use std::{
    io::IsTerminal,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;

use crate::{
    agg::{Aggregator, SetRow},
    config::Config,
    leader::LeaderSchedule,
    live::LiveStats,
    out::{Archive, Counters, Manifest, TxnCompareSummary},
    pinger::NetMon,
    registry::Registry,
    rx::RxStats,
    tui::Tui,
    verify::{verify_chunk, VerifyStats},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const SCHEMA_VERSION: u32 = 2;

#[derive(Parser)]
#[command(name = "shred-audit", version)]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: String,
    /// Also emit shreds.parquet — one row per shred. Large: ~60M rows / ~1GB
    /// per 10 minutes at 100 kpps.
    #[arg(long)]
    dump_shreds: bool,
    /// Stop after this many seconds. 0 = run until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// Disable the live TUI dashboard and print the periodic status line instead.
    /// The TUI is the default on a real terminal; it falls back to the status
    /// line when stdout is not a terminal (piped, nohup, systemd).
    #[arg(long)]
    no_tui: bool,
    /// Realtime mode: also refresh a stable `<output_dir>/live.zip` every
    /// `live_secs` (config, default 10) with the current window's data, for the
    /// web viewer's "watch URL" mode. Rotation into timestamped archives is
    /// unaffected. Serve the dir yourself (e.g. `python3 -m http.server`).
    #[arg(long)]
    live: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).context("loading config")?;
    let registry = Arc::new(Registry::build(&cfg));

    eprintln!(
        "shred-audit {VERSION}: {} providers, ports {:?}, {} verify threads",
        registry.len(),
        cfg.listen_ports,
        cfg.verify_thread_count()
    );

    let schedule = LeaderSchedule::new(&cfg.rpc_url);
    schedule
        .refresh()
        .context("initial leader schedule fetch — check rpc_url")?;
    // Validator names for the viewer. Best-effort; never fatal.
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
        ctrlc::set_handler(move || {
            eprintln!("\nshutting down, flushing archive...");
            exit.store(true, Ordering::Relaxed);
        })?;
    }

    let rx_stats = Arc::new(RxStats::default());
    // Bounded: if verification falls behind we drop and *count*, never grow
    // unbounded and start reporting our own backlog as network latency.
    let (tx, rx_chan) = crossbeam_channel::bounded::<Vec<rx::Packet>>(16_384);

    // Source IPs per provider (from the rx threads) and their ping RTTs.
    let netmon = Arc::new(pinger::NetMon::new());

    let _rx_handles = rx::spawn_receivers(
        cfg.bind_ip,
        &cfg.listen_ports,
        registry.clone(),
        netmon.clone(),
        tx,
        rx_stats.clone(),
        exit.clone(),
    )
    .context("binding sockets")?;

    let _ping_handle = pinger::spawn(
        netmon.clone(),
        cfg.clone(),
        registry.clone(),
        exit.clone(),
    );

    let out_dir = PathBuf::from(&cfg.output_dir);
    std::fs::create_dir_all(&out_dir)?;

    let mut vstats = VerifyStats::default();
    let mut aggregator = Aggregator::new(cfg.fec_max_wait_slots);

    // Optional shred-vs-gRPC transaction-timing comparison. `None` unless the
    // config declares one or more `grpc_sources`.
    let txn_compare = txncmp::TxnCompare::start(&cfg);

    let mut archive_start = out::now_unix_ns();
    let mut work_dir = out_dir.join(format!(".work-{archive_start}"));
    let mut archive = Archive::create(&work_dir, args.dump_shreds)?;

    let started = Instant::now();
    let mut last_report = Instant::now();
    // Cached comparison snapshot, refreshed on a timer so the TUI and live viewer
    // don't recompute it every frame over the full signature set.
    let mut last_txn = Instant::now();
    let mut txn_snap: Option<TxnCompareSummary> = txn_compare.as_ref().map(|tc| tc.snapshot());
    let mut last_rotate = Instant::now();
    let mut last_sched_check = Instant::now();
    let mut last_draw = Instant::now();
    let mut archives: Vec<PathBuf> = Vec::new();

    // Realtime mode (--live): accumulate the window's finalized rows and refresh
    // live.zip every `live_secs`. window_rows is cleared on rotation, so it holds
    // at most one window; with rotate_secs = 0 it grows for the whole run, so set
    // a modest rotate_secs for long live captures.
    let live_secs = if cfg.live_secs == 0 { 10 } else { cfg.live_secs };
    let mut window_rows: Vec<SetRow> = Vec::new();
    let mut last_live = Instant::now();
    if args.live {
        eprintln!(
            "live mode: refreshing {}/live.zip every {live_secs}s — serve that dir and point the \
             viewer's watch-URL at live.zip",
            out_dir.display()
        );
        if cfg.rotate_secs == 0 {
            eprintln!(
                "  note: rotate_secs = 0, so the live window (and live.zip) grows for the whole \
                 run; set rotate_secs for a bounded live snapshot"
            );
        }
    }

    // Live comparison + dashboard (opt-in). Entering the TUI takes over the
    // terminal, so it happens only after the startup warnings have printed.
    let mut live = LiveStats::new();
    let mut footer = String::new();
    // The dashboard takes over the terminal, so it runs only on a real TTY, never
    // when output is piped or redirected, where it would spew escape codes.
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
                if let Some(tc) = &txn_compare {
                    for p in &packets {
                        tc.feed(p.rx_unix_ns, p.provider, &p.data);
                    }
                }
                let verified =
                    verify_chunk(packets, &schedule, cfg.shred_version, &mut vstats);
                for s in &verified {
                    aggregator.ingest(s);
                }
                if args.dump_shreds {
                    archive.write_shreds(&registry, &verified)?;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        let rows = aggregator.harvest(false);
        if !rows.is_empty() {
            if tui.is_some() {
                live.ingest(&rows);
            }
            if args.live {
                window_rows.extend_from_slice(&rows);
            }
            archive.write_sets(&registry, &rows)?;
        }

        if last_sched_check.elapsed() >= Duration::from_secs(30) {
            last_sched_check = Instant::now();
            if aggregator.pending_sets() > 0 && schedule.needs_refresh(archive.max_slot) {
                if let Err(e) = schedule.refresh() {
                    let msg = format!("leader schedule refresh failed: {e:#}");
                    if tui.is_some() {
                        footer = msg;
                    } else {
                        eprintln!("{msg}");
                    }
                }
            }
        }

        // Refresh the comparison snapshot on a timer (not every frame).
        if txn_compare.is_some() && last_txn.elapsed() >= Duration::from_secs(2) {
            last_txn = Instant::now();
            txn_snap = txn_compare.as_ref().map(|tc| tc.snapshot());
        }

        // Live dashboard: poll for quit and repaint a few times a second;
        // otherwise the periodic status line is the non-interactive fallback.
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
            report(&rx_stats, &vstats, &aggregator, archive.invalid_data);
        }

        if args.live && last_live.elapsed() >= Duration::from_secs(live_secs) {
            last_live = Instant::now();
            if let Err(e) = write_live_snapshot(
                &out_dir, &window_rows, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
                txn_snap.as_ref(), aggregator.shreds_after_window(), archive_start,
            ) {
                let msg = format!("live snapshot failed: {e:#}");
                if tui.is_some() {
                    footer = msg;
                } else {
                    eprintln!("{msg}");
                }
            }
        }

        if cfg.rotate_secs > 0 && last_rotate.elapsed() >= Duration::from_secs(cfg.rotate_secs) {
            last_rotate = Instant::now();
            let rows = aggregator.harvest(true);
            if tui.is_some() {
                live.ingest(&rows);
            }
            archive.write_sets(&registry, &rows)?;
            let zip = finish_archive(
                archive, &out_dir, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
                txn_snap.as_ref(), aggregator.shreds_after_window(), archive_start,
            )?;
            if tui.is_some() {
                footer = format!("wrote {}", zip.display());
            } else {
                eprintln!("wrote {}", zip.display());
            }
            archives.push(zip);

            archive_start = out::now_unix_ns();
            work_dir = out_dir.join(format!(".work-{archive_start}"));
            archive = Archive::create(&work_dir, args.dump_shreds)?;
            // A new window begins; live.zip now tracks it from empty.
            window_rows.clear();
        }
    }

    // Restore the terminal before any shutdown output goes to stderr.
    drop(tui.take());

    // Drain whatever the receivers already queued before we tear down.
    exit.store(true, Ordering::Relaxed);
    while let Ok(packets) = rx_chan.try_recv() {
        if let Some(tc) = &txn_compare {
            for p in &packets {
                tc.feed(p.rx_unix_ns, p.provider, &p.data);
            }
        }
        let verified = verify_chunk(packets, &schedule, cfg.shred_version, &mut vstats);
        for s in &verified {
            aggregator.ingest(s);
        }
        if args.dump_shreds {
            archive.write_shreds(&registry, &verified)?;
        }
    }
    let rows = aggregator.harvest(true);
    archive.write_sets(&registry, &rows)?;
    // Final comparison snapshot so the last archive reflects the whole run.
    if let Some(tc) = &txn_compare {
        txn_snap = Some(tc.final_snapshot());
    }
    // Refresh live.zip once more so it reflects the final state, not the
    // second-to-last snapshot.
    if args.live {
        window_rows.extend_from_slice(&rows);
        if let Err(e) = write_live_snapshot(
            &out_dir, &window_rows, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
            txn_snap.as_ref(), aggregator.shreds_after_window(), archive_start,
        ) {
            eprintln!("final live snapshot failed: {e:#}");
        }
    }
    let bad_data = archive.invalid_data;
    let zip = finish_archive(
        archive, &out_dir, &cfg, &registry, &netmon, &schedule, &rx_stats, &vstats,
        txn_snap.as_ref(), aggregator.shreds_after_window(), archive_start,
    )?;
    eprintln!("wrote {}", zip.display());
    archives.push(zip);

    report(&rx_stats, &vstats, &aggregator, bad_data);

    // Stop the gRPC/deshred subsystem and print the transaction-timing summary.
    if let Some(tc) = txn_compare {
        tc.finish();
    }

    eprintln!("\n{} archive(s):", archives.len());
    for a in &archives {
        eprintln!("  {}", a.display());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_archive(
    archive: Archive,
    out_dir: &std::path::Path,
    cfg: &Config,
    registry: &Registry,
    netmon: &NetMon,
    schedule: &LeaderSchedule,
    rx_stats: &RxStats,
    vstats: &VerifyStats,
    txn: Option<&TxnCompareSummary>,
    shreds_after_window: u64,
    started_at: i64,
) -> Result<PathBuf> {
    let manifest = build_manifest(
        &archive, cfg, registry, netmon, schedule, rx_stats, vstats, txn, shreds_after_window,
        started_at,
    );
    let name = format!(
        "shred-audit-{}-{}.zip",
        chrono::DateTime::from_timestamp_nanos(started_at).format("%Y%m%dT%H%M%SZ"),
        manifest.hostname
    );
    archive.finish(&out_dir.join(name), manifest)
}

/// Write/refresh the stable `live.zip` snapshot atomically (temp file + rename,
/// so a watcher never reads a half-written zip). `rows` is the current window's
/// finalized sets; the run's full history still lands in the rotated archives.
#[allow(clippy::too_many_arguments)]
fn write_live_snapshot(
    out_dir: &std::path::Path,
    rows: &[SetRow],
    cfg: &Config,
    registry: &Registry,
    netmon: &NetMon,
    schedule: &LeaderSchedule,
    rx_stats: &RxStats,
    vstats: &VerifyStats,
    txn: Option<&TxnCompareSummary>,
    shreds_after_window: u64,
    window_start: i64,
) -> Result<()> {
    let work = out_dir.join(".live-work");
    let mut a = Archive::create(&work, false)?;
    a.write_sets(registry, rows)?;
    let mut manifest = build_manifest(
        &a, cfg, registry, netmon, schedule, rx_stats, vstats, txn, shreds_after_window,
        window_start,
    );
    manifest.notes.insert(
        0,
        "LIVE snapshot — an in-progress capture window, refreshed periodically. It is replaced \
         atomically each refresh; the full run lands in the rotated timestamped archives."
            .to_string(),
    );
    let tmp = out_dir.join(".live.zip.tmp");
    a.finish(&tmp, manifest)?;
    std::fs::rename(&tmp, out_dir.join("live.zip"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_manifest(
    archive: &Archive,
    cfg: &Config,
    registry: &Registry,
    netmon: &NetMon,
    schedule: &LeaderSchedule,
    rx_stats: &RxStats,
    vstats: &VerifyStats,
    txn: Option<&TxnCompareSummary>,
    shreds_after_window: u64,
    started_at: i64,
) -> Manifest {
    let mut notes = Vec::new();
    let no_ts = rx_stats.no_timestamp.load(Ordering::Relaxed);
    if no_ts > 0 {
        notes.push(format!(
            "{no_ts} datagrams arrived with no SCM_TIMESTAMPNS control message and were discarded; \
             timings in this archive are from the remainder only"
        ));
    }
    let full = rx_stats.channel_full.load(Ordering::Relaxed);
    if full > 0 {
        notes.push(format!(
            "{full} receive batches were dropped because the verify queue was full; \
             this machine could not keep up and coverage is incomplete"
        ));
    }
    let kernel_dropped = rx_stats.kernel_dropped.load(Ordering::Relaxed);
    if kernel_dropped > 0 {
        notes.push(format!(
            "{kernel_dropped} datagrams were dropped by the KERNEL from this host's socket queue              (SO_RXQ_OVFL) — they never reached the tool. This is OUR loss, not a provider's: the              shreds in them are absent from the data and inflate `missed` for whichever provider              sent them. Do not read that as provider packet loss. Raise net.core.rmem_max and/or              reduce load on this host, then re-capture"
        ));
    }
    let truncated = rx_stats.truncated.load(Ordering::Relaxed);
    if truncated > 0 {
        notes.push(format!(
            "{truncated} datagrams were larger than any Solana shred and were truncated by the              kernel; they were discarded rather than parsed. A provider sending these is probably              not sending one raw shred per datagram (batching, or an encapsulating header), and              none of its traffic of that shape is represented here"
        ));
    }
    if vstats.unsupported_variant > 0 {
        notes.push(format!(
            "{} shreds used a shred variant this build cannot parse (legacy, or newer than this              binary). They are counted here and excluded from every verdict — they are NOT counted              as invalid. If this number is large, this tool is out of date, not your provider",
            vstats.unsupported_variant
        ));
    }
    let unmatched = rx_stats.unmatched.load(Ordering::Relaxed);
    if unmatched > 0 {
        notes.push(format!(
            "{unmatched} datagrams matched no provider rule and were ignored"
        ));
    }
    if shreds_after_window > 0 {
        notes.push(format!(
            "{shreds_after_window} shreds arrived for FEC sets that had already been finalized \
             (their slot was past the {}-slot window) and were dropped — they could not be added \
             to a set already written out. A large count means the window is too short for a slow \
             or reordering provider, or that a provider is lagging; its late deliveries are absent \
             here and inflate its `missed`. Do not read that as the provider sending nothing",
            cfg.fec_max_wait_slots
        ));
    }
    if archive.invalid_data > 0 {
        notes.push(format!(
            "{} shreds carried block data that differs from the leader-signed copy of the same \
             shred. This is NOT a broken merkle proof over genuine data — the content itself is \
             not what the leader signed. Treat it as a substitution until proven otherwise",
            archive.invalid_data
        ));
    }
    if archive.invalid_sig > 0 {
        notes.push(format!(
            "{} shreds failed verification but carry the leader's genuine block data — their \
             merkle proof does not reconstruct the signed root. The data is authentic; the proof \
             of it is not, so the shred cannot be authenticated and agave will reject it",
            archive.invalid_sig
        ));
    }
    if archive.invalid_unknown > 0 {
        notes.push(format!(
            "{} shreds failed verification and no provider delivered a leader-authenticated copy \
             of the same shred, so they could not be classified as bad-signature or bad-data. \
             They are counted only under `invalid_unknown`, never folded into either",
            archive.invalid_unknown
        ));
    }
    if vstats.no_leader > 0 {
        notes.push(format!(
            "{} shreds had no known leader (schedule gap) and were counted as unverifiable, \
             not as invalid",
            vstats.no_leader
        ));
    }

    Manifest {
        tool: "shred-audit",
        tool_version: VERSION,
        schema_version: SCHEMA_VERSION,
        hostname: hostname(),
        started_at_unix_ns: started_at,
        ended_at_unix_ns: out::now_unix_ns(),
        clock_source: "SO_TIMESTAMPNS (kernel, CLOCK_REALTIME, stamped at driver handoff)",
        timestamp_semantics: "absolute unix nanoseconds; provider deltas are exact subtractions \
                              on a single host clock, no baseline provider involved",
        providers: registry.names().to_vec(),
        rpc_url: cfg.rpc_url.clone(),
        leader_schedule_epoch: schedule.epoch(),
        min_slot: if archive.min_slot == u64::MAX { 0 } else { archive.min_slot },
        max_slot: archive.max_slot,
        rows_fec_sets: archive.rows_sets,
        rows_shreds: archive.rows_shreds,
        provider_pings: netmon.provider_pings(cfg, registry),
        leader_names: schedule.leader_names(),
        txn_compare: txn.cloned(),
        counters: Counters {
            udp_received: rx_stats.received.load(Ordering::Relaxed),
            udp_unmatched: unmatched,
            udp_no_timestamp: no_ts,
            udp_channel_full: full,
            udp_kernel_dropped: kernel_dropped,
            udp_truncated: truncated,
            shreds_parsed: vstats.parsed,
            shreds_malformed: vstats.malformed,
            shreds_unsupported_variant: vstats.unsupported_variant,
            non_shred_pings: vstats.non_shred_ping,
            shreds_wrong_version: vstats.wrong_version,
            shreds_no_merkle_root: vstats.no_merkle_root,
            shreds_no_leader: vstats.no_leader,
            shreds_sig_bad: vstats.sig_bad,
            invalid_sig: archive.invalid_sig,
            invalid_data: archive.invalid_data,
            invalid_unknown: archive.invalid_unknown,
            ed25519_verifies: vstats.ed25519_verifies,
            batch_fallbacks: vstats.batch_fallbacks,
            shreds_after_window,
        },
        notes,
    }
}

fn report(rx_stats: &RxStats, v: &VerifyStats, agg: &Aggregator, bad_data: u64) {
    eprintln!(
        "rx {} (unmatched {}, no_ts {}, dropped {}, kernel_drop {}, trunc {}) | parsed {} bad_sig {} (data {}) no_leader {} malformed {} ping {} unsupported {} | ed25519 {} (batch_fallback {}) | after_window {} | pending sets {}",
        rx_stats.received.load(Ordering::Relaxed),
        rx_stats.unmatched.load(Ordering::Relaxed),
        rx_stats.no_timestamp.load(Ordering::Relaxed),
        rx_stats.channel_full.load(Ordering::Relaxed),
        rx_stats.kernel_dropped.load(Ordering::Relaxed),
        rx_stats.truncated.load(Ordering::Relaxed),
        v.parsed,
        v.sig_bad,
        bad_data,
        v.no_leader,
        v.malformed,
        v.non_shred_ping,
        v.unsupported_variant,
        v.ed25519_verifies,
        v.batch_fallbacks,
        agg.shreds_after_window(),
        agg.pending_sets(),
    );
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}
