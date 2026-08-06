mod agg;
mod config;
mod deshred;
mod grpc;
mod leader;
mod live;
mod mcast;
mod names;
mod out;
mod pinger;
mod proto;
mod registry;
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
    mcast::MembershipLog,
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
    for m in &cfg.multicast {
        eprintln!(
            "  multicast port {}: groups {:?} on interface {} ({})",
            m.port,
            m.resolved_groups(),
            m.interface,
            if m.require_route {
                "joined once their DoubleZero host route appears"
            } else {
                "joined unconditionally (require_route: false)"
            },
        );
    }

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

    // Multicast group membership, empty and inert unless the config declares a
    // `multicast` block.
    let membership = Arc::new(MembershipLog::new());

    let _rx_handles = rx::spawn_receivers(
        cfg.bind_ip,
        &cfg.listen_ports,
        &cfg.multicast,
        registry.clone(),
        netmon.clone(),
        tx,
        rx_stats.clone(),
        membership.clone(),
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
                &out_dir, &window_rows, &cfg, &registry, &netmon, &membership, &schedule, &rx_stats, &vstats,
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
                archive, &out_dir, &cfg, &registry, &netmon, &membership, &schedule, &rx_stats, &vstats,
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
            // A new window begins; live.zip now tracks it from empty, and
            // multicast joined-time is re-accounted against the new window.
            window_rows.clear();
            membership.begin_window(archive_start);
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
            &out_dir, &window_rows, &cfg, &registry, &netmon, &membership, &schedule, &rx_stats, &vstats,
            txn_snap.as_ref(), aggregator.shreds_after_window(), archive_start,
        ) {
            eprintln!("final live snapshot failed: {e:#}");
        }
    }
    let bad_data = archive.invalid_data;
    let zip = finish_archive(
        archive, &out_dir, &cfg, &registry, &netmon, &membership, &schedule, &rx_stats, &vstats,
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
    membership: &MembershipLog,
    schedule: &LeaderSchedule,
    rx_stats: &RxStats,
    vstats: &VerifyStats,
    txn: Option<&TxnCompareSummary>,
    shreds_after_window: u64,
    started_at: i64,
) -> Result<PathBuf> {
    let manifest = build_manifest(
        &archive, cfg, registry, netmon, membership, schedule, rx_stats, vstats, txn,
        shreds_after_window, started_at,
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
    membership: &MembershipLog,
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
        &a, cfg, registry, netmon, membership, schedule, rx_stats, vstats, txn,
        shreds_after_window, window_start,
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
    membership: &MembershipLog,
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
    if let Some(t) = txn {
        notes.extend(onchain_notes(cfg, t));
    }

    // Multicast membership, and the caveats that follow from it. A provider fed
    // by a group that was not joined has no data for the period it was absent,
    // and every comparison against it is wrong by exactly that much.
    let multicast = (!cfg.multicast.is_empty()).then(|| membership.snapshot());
    if let Some(m) = &multicast {
        notes.extend(multicast_notes(m, started_at, out::now_unix_ns()));
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
        multicast,
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

/// Caveats for multicast membership.
///
/// A multicast provider can only be compared over the time its groups were
/// actually joined. Membership is not a detail of the plumbing: while a group is
/// left, its provider receives nothing, and every set the other providers
/// delivered in that period counts against it as `missed` and as a set it was
/// absent for. Presented without this, a DoubleZero tunnel that was down for
/// half a capture is indistinguishable from a transport that lost half the
/// races — so say it outright.
fn multicast_notes(m: &out::MulticastStatus, window_start: i64, now: i64) -> Vec<String> {
    let mut notes = Vec::new();
    let window_ns = (now - window_start).max(1);

    let never: Vec<&str> = m
        .groups
        .iter()
        .filter(|g| g.first_joined_at_unix_ns.is_none())
        .map(|g| g.group.as_str())
        .collect();
    if never.len() == m.groups.len() && !never.is_empty() {
        notes.push(format!(
            "NO multicast group was ever joined ({}). The provider on those ports received \
             nothing, so it did not lose any race — it was in none. Check that the DoubleZero \
             tunnel is up and a /32 host route to each group exists (`ip route get <group>`), or \
             set `require_route: false` if this deployment installs no such route",
            never.join(", ")
        ));
    } else if !never.is_empty() {
        notes.push(format!(
            "multicast group(s) {} were never joined, so nothing arrived from them. Any group \
             traffic they carry is absent from this archive",
            never.join(", ")
        ));
    }

    for g in &m.groups {
        if g.join_errors > 0 {
            notes.push(format!(
                "multicast group {} failed to join {} time(s) (last: {}). While unjoined it \
                 delivered nothing",
                g.group,
                g.join_errors,
                g.last_error.as_deref().unwrap_or("unknown"),
            ));
        }
        // Only meaningful for a group that did join at some point; one that never
        // joined is already covered above.
        if g.first_joined_at_unix_ns.is_some() {
            let pct = 100.0 * g.joined_ns as f64 / window_ns as f64;
            if pct < 99.0 {
                notes.push(format!(
                    "multicast group {} was joined for only {:.1}% of this window ({} join(s), {} \
                     leave(s)). Its provider was absent for the remainder, which inflates that \
                     provider's `missed` and depresses its coverage for reasons that have nothing \
                     to do with delivery speed. Compare providers over a window where membership \
                     was continuous, or restrict the analysis to slots inside the joined intervals \
                     listed under `multicast.events`",
                    g.group, pct, g.joins, g.leaves,
                ));
            }
        }
    }

    if m.events_dropped > 0 {
        notes.push(format!(
            "{} multicast membership transition(s) were dropped from `multicast.events` after the \
             retention cap — the tunnel flapped more than the event list holds. The per-group \
             `joins`/`leaves`/`joined_ns` counters are still exact",
            m.events_dropped
        ));
    }
    notes
}

/// Caveats for the onchain transaction audit. It is a *sample* — one slot every
/// `onchain_sample_secs` against ~2.5 produced a second — so the archive has to
/// say what was sampled and what was not, or its counts read as totals.
fn onchain_notes(cfg: &Config, txn: &TxnCompareSummary) -> Vec<String> {
    let mut notes = Vec::new();
    if !cfg.onchain_verify {
        notes.push(
            "the onchain transaction audit was disabled (`onchain_verify: false`), so nothing here \
             checks that the transactions each source delivered are the ones the cluster actually \
             produced. Every `onchain_*` field is zero because the check did not run — not because \
             the sources were clean"
                .to_string(),
        );
        return notes;
    }
    if txn.onchain_slots_checked == 0 {
        notes.push(format!(
            "the onchain transaction audit ran but compared no slot ({} rpc errors, {} slots the \
             cluster produced no block for). Every `onchain_*` field is zero because nothing was \
             checked. Last error: {}",
            txn.onchain_rpc_errors,
            txn.onchain_slots_unavailable,
            txn.onchain_last_error.as_deref().unwrap_or("none"),
        ));
        return notes;
    }
    notes.push(format!(
        "the onchain transaction audit compared {} sampled slots (one every {}s, taken {} slots \
         behind the tip) against getBlock on {}. `onchain_missed` / `onchain_corrupted` / \
         `onchain_duplicated` are counts over those slots only — compare sources on \
         `onchain_bad_pct`, not on the raw counts",
        txn.onchain_slots_checked,
        cfg.onchain_sample_secs,
        cfg.onchain_lag_slots,
        cfg.effective_onchain_rpc_url(),
    ));
    if txn.onchain_rpc_errors > 0 {
        notes.push(format!(
            "{} getBlock calls failed during the audit, so fewer slots were sampled than the \
             capture length suggests. The rates are still over the slots that did land, but a \
             rate-limited endpoint biases WHICH slots those were — give `onchain_rpc_url` its own \
             node if this is large. Last error: {}",
            txn.onchain_rpc_errors,
            txn.onchain_last_error.as_deref().unwrap_or("unknown"),
        ));
    }
    for s in &txn.sources {
        if s.onchain_slots_absent > 0 && s.onchain_slots_checked == 0 {
            notes.push(format!(
                "source `{}` delivered nothing for any of the {} sampled slots and was never \
                 scored against the chain. A feed subscribed at a commitment that lags past the \
                 {}-slot sampling window looks exactly like this — it is not a finding about the \
                 source's data",
                s.name, s.onchain_slots_absent, cfg.onchain_lag_slots,
            ));
        }
    }
    if let Some(worst) = txn
        .sources
        .iter()
        .filter(|s| s.onchain_corrupted > 0)
        .max_by_key(|s| s.onchain_corrupted)
    {
        notes.push(format!(
            "at least one source delivered transactions the sampled blocks do not contain \
             (`{}`: {} of {}). A pre-execution deshred feed can legitimately do this for a \
             transaction that never landed, and a `processed` subscription can do it across a \
             dropped fork — read it next to `onchain_missed` before treating it as fabrication",
            worst.name, worst.onchain_corrupted, worst.onchain_txns,
        ));
    }
    notes
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
