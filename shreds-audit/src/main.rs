mod agg;
mod config;
mod leader;
mod live;
mod out;
mod registry;
mod rx;
mod tui;
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
    agg::Aggregator,
    config::Config,
    leader::LeaderSchedule,
    live::LiveStats,
    out::{Archive, Counters, Manifest},
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
    /// The TUI is the default on a real terminal; it always falls back to the
    /// status line when stdout is not a terminal (piped, nohup, systemd).
    #[arg(long)]
    no_tui: bool,
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

    let _rx_handles = rx::spawn_receivers(
        cfg.bind_ip,
        &cfg.listen_ports,
        registry.clone(),
        tx,
        rx_stats.clone(),
        exit.clone(),
    )
    .context("binding sockets")?;

    let out_dir = PathBuf::from(&cfg.output_dir);
    std::fs::create_dir_all(&out_dir)?;

    let mut vstats = VerifyStats::default();
    let mut aggregator = Aggregator::new(cfg.fec_max_wait_slots);

    let mut archive_start = out::now_unix_ns();
    let mut work_dir = out_dir.join(format!(".work-{archive_start}"));
    let mut archive = Archive::create(&work_dir, args.dump_shreds)?;

    let started = Instant::now();
    let mut last_report = Instant::now();
    let mut last_rotate = Instant::now();
    let mut last_sched_check = Instant::now();
    let mut last_draw = Instant::now();
    let mut archives: Vec<PathBuf> = Vec::new();

    // Live comparison + dashboard (opt-in). Entering the TUI takes over the
    // terminal, so it happens only after the startup warnings have printed.
    let mut live = LiveStats::new();
    let mut footer = String::new();
    // The dashboard is the default, but it takes over the terminal — so it runs
    // only on a real TTY, never when output is piped or redirected (nohup, systemd,
    // a log file), where it would spew escape codes or fail to start outright.
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

        // Live dashboard: poll for quit and repaint a few times a second. When it
        // is off, the plain periodic status line is the non-interactive fallback.
        if let Some(t) = tui.as_mut() {
            if t.quit_requested()? {
                exit.store(true, Ordering::Relaxed);
            }
            if last_draw.elapsed() >= Duration::from_millis(400) {
                last_draw = Instant::now();
                t.draw(&live, &registry, &footer)?;
            }
        } else if last_report.elapsed() >= Duration::from_secs(10) {
            last_report = Instant::now();
            report(&rx_stats, &vstats, &aggregator, archive.invalid_data);
        }

        if cfg.rotate_secs > 0 && last_rotate.elapsed() >= Duration::from_secs(cfg.rotate_secs) {
            last_rotate = Instant::now();
            let rows = aggregator.harvest(true);
            if tui.is_some() {
                live.ingest(&rows);
            }
            archive.write_sets(&registry, &rows)?;
            let zip = finish_archive(
                archive, &out_dir, &cfg, &registry, &schedule, &rx_stats, &vstats,
                aggregator.shreds_after_window(), archive_start,
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
        }
    }

    // Restore the terminal before any shutdown output goes to stderr.
    drop(tui.take());

    // Drain whatever the receivers already queued before we tear down.
    exit.store(true, Ordering::Relaxed);
    while let Ok(packets) = rx_chan.try_recv() {
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
    let bad_data = archive.invalid_data;
    let zip = finish_archive(
        archive, &out_dir, &cfg, &registry, &schedule, &rx_stats, &vstats,
        aggregator.shreds_after_window(), archive_start,
    )?;
    eprintln!("wrote {}", zip.display());
    archives.push(zip);

    report(&rx_stats, &vstats, &aggregator, bad_data);
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
    schedule: &LeaderSchedule,
    rx_stats: &RxStats,
    vstats: &VerifyStats,
    shreds_after_window: u64,
    started_at: i64,
) -> Result<PathBuf> {
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

    let manifest = Manifest {
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
    };

    let name = format!(
        "shred-audit-{}-{}.zip",
        chrono::DateTime::from_timestamp_nanos(started_at).format("%Y%m%dT%H%M%SZ"),
        manifest.hostname
    );
    archive.finish(&out_dir.join(name), manifest)
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
