//! Orchestration for the shred-vs-gRPC transaction-timing comparison.
//!
//! Wires three pieces around a shared [`SigRegistry`]:
//!   * a deshred worker thread reconstructing transactions from the shred stream,
//!   * a Tokio runtime on its own thread running every configured gRPC source,
//!   * a summary printed at shutdown.
//!
//! Created only when the config declares `grpc_sources`.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Sender;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::deshred::{is_data_shred_variant, Deshredder, ShredInput};
use crate::sigreg::{SigRegistry, SourceKind};

/// Offset of the shred variant byte within a shred payload. Matches `verify.rs`.
const VARIANT_OFFSET: usize = 64;

pub struct TxnCompare {
    reg: Arc<Mutex<SigRegistry>>,
    feed: Sender<ShredInput>,
    deshred_handle: Option<JoinHandle<()>>,
    grpc_handle: Option<JoinHandle<()>>,
    cancel: CancellationToken,
}

impl TxnCompare {
    /// Spin up the comparison subsystem. Returns `None` when no gRPC sources are
    /// configured, so callers can treat "off" as the common case.
    pub fn start(cfg: &Config) -> Option<Self> {
        if cfg.grpc_sources.is_empty() {
            return None;
        }

        // Each shred provider is its own source (source id = provider id); gRPC
        // feeds follow. Both race each transaction as peers.
        let mut names: Vec<String> = cfg.providers.iter().map(|p| p.name.clone()).collect();
        let mut kinds: Vec<SourceKind> = vec![SourceKind::Shred; cfg.providers.len()];
        let n_providers = cfg.providers.len();
        for g in &cfg.grpc_sources {
            names.push(g.name.clone());
            kinds.push(SourceKind::Grpc);
        }
        let reg = Arc::new(Mutex::new(SigRegistry::new(names, kinds)));

        // Deshred worker. Bounded so a stall drops feed rather than growing without
        // limit; the comparison is best-effort and must never become the backlog.
        let (feed, feed_rx) = crossbeam_channel::bounded::<ShredInput>(65_536);
        let settle = Duration::from_secs(cfg.txn_settle_secs.max(1));
        let deshredder = Deshredder::new(reg.clone(), settle);
        let deshred_handle = std::thread::Builder::new()
            .name("deshred".into())
            .spawn(move || deshredder.run(feed_rx))
            .ok();

        // gRPC runtime thread. gRPC source ids start after the shred providers.
        let cancel = CancellationToken::new();
        let grpc_sources = cfg.grpc_sources.clone();
        let grpc_reg = reg.clone();
        let grpc_cancel = cancel.clone();
        let grpc_handle = std::thread::Builder::new()
            .name("grpc".into())
            .spawn(move || run_grpc_runtime(grpc_sources, grpc_reg, grpc_cancel, n_providers))
            .ok();

        eprintln!(
            "txn-compare: reconstructing transactions per shred provider and subscribing to {} \
             gRPC source(s); racing every source by transaction signature",
            cfg.grpc_sources.len()
        );

        Some(Self {
            reg,
            feed,
            deshred_handle,
            grpc_handle,
            cancel,
        })
    }

    /// Current comparison as a structured snapshot — fed to the TUI live and
    /// embedded in the archive manifest for the result page and web viewer.
    /// Retires signatures that have settled, keeping the registry bounded.
    pub fn snapshot(&self) -> crate::out::TxnCompareSummary {
        build_snapshot(&self.reg, false)
    }

    /// Like [`snapshot`](Self::snapshot) but finalizes every in-flight signature,
    /// so the last archive reflects the whole run rather than dropping the tail
    /// still within the eviction margin.
    pub fn final_snapshot(&self) -> crate::out::TxnCompareSummary {
        build_snapshot(&self.reg, true)
    }

    /// Tee a received datagram into the deshred feed if it looks like a data shred.
    /// Cheap: one byte is inspected before any clone, so coding shreds, pings, and
    /// non-shred traffic never allocate. Silently drops when the feed is full.
    pub fn feed(&self, rx_unix_ns: i64, provider: u16, data: &[u8]) {
        if data.len() <= VARIANT_OFFSET {
            return;
        }
        if !is_data_shred_variant(data[VARIANT_OFFSET]) {
            return;
        }
        let _ = self.feed.try_send(ShredInput {
            rx_unix_ns,
            provider,
            data: data.to_vec(),
        });
    }

    /// Stop the subsystem, join its threads, and print the timing summary.
    pub fn finish(self) {
        let TxnCompare {
            reg,
            feed,
            mut deshred_handle,
            mut grpc_handle,
            cancel,
        } = self;
        // Dropping the sender lets the deshred worker drain and exit; cancel stops
        // the gRPC runtime.
        cancel.cancel();
        drop(feed);
        if let Some(h) = deshred_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = grpc_handle.take() {
            let _ = h.join();
        }
        report(&reg);
    }
}

/// Build a structured snapshot from the shared registry; every source becomes a
/// peer row. The registry lock is held only long enough to finalize settled rows
/// and copy each source's raw totals — the per-source percentile sort runs after
/// the lock is released, off the hot `record_first` path.
fn build_snapshot(reg: &Mutex<SigRegistry>, force: bool) -> crate::out::TxnCompareSummary {
    use crate::out::{TxnCompareSummary, TxnSource};
    use crate::sigreg::{percentiles_us, SourceKind};

    struct Row {
        name: String,
        kind: SourceKind,
        raw: crate::sigreg::SourceRaw,
    }

    let (rows, distinct, contested) = {
        let mut reg = reg.lock().unwrap();
        reg.finalize(force);
        let raw = reg.export();
        let rows: Vec<Row> = raw
            .into_iter()
            .enumerate()
            .map(|(sid, raw)| Row {
                name: reg.name(sid).to_string(),
                kind: reg.kind(sid),
                raw,
            })
            .collect();
        (rows, reg.distinct_signatures(), reg.contested_signatures())
    };

    let sources = rows
        .into_iter()
        .map(|mut r| {
            let pct = percentiles_us(&mut r.raw.behind_ns);
            TxnSource {
                name: r.name,
                kind: match r.kind {
                    SourceKind::Shred => "shreds".into(),
                    SourceKind::Grpc => "grpc".into(),
                },
                seen: r.raw.seen,
                contested: r.raw.contested,
                winrate: (r.raw.contested > 0).then(|| r.raw.wins as f64 / r.raw.contested as f64),
                behind_mean_us: r.raw.mean_us,
                behind_p50_us: pct.p50,
                behind_p90_us: pct.p90,
                behind_p99_us: pct.p99,
            }
        })
        .collect();
    TxnCompareSummary {
        distinct_signatures: distinct,
        contested,
        sources,
    }
}

/// One-line-per-source summary to stderr at shutdown (logs only; the real output
/// is the snapshot embedded in the manifest).
fn report(reg: &Mutex<SigRegistry>) {
    let snap = build_snapshot(reg, true);
    eprintln!(
        "\ntxn-compare: {} distinct signatures, {} contested",
        snap.distinct_signatures, snap.contested
    );
    for s in &snap.sources {
        eprintln!(
            "  {:<20} winrate={} µs behind: p50={:.1} p90={:.1} p99={:.1} (seen {})",
            s.name,
            s.winrate
                .map(|w| format!("{:.1}%", w * 100.0))
                .unwrap_or_else(|| "—".into()),
            s.behind_p50_us.unwrap_or(0.0),
            s.behind_p90_us.unwrap_or(0.0),
            s.behind_p99_us.unwrap_or(0.0),
            s.seen,
        );
    }
}

/// Run every gRPC source on a dedicated multi-thread runtime, each supervised with
/// a reconnect loop, until cancellation.
fn run_grpc_runtime(
    sources: Vec<crate::config::GrpcSourceCfg>,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
    sid_base: usize,
) {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("txn-compare: could not start gRPC runtime: {e}");
            return;
        }
    };

    rt.block_on(async move {
        let mut set = tokio::task::JoinSet::new();
        for (i, src) in sources.into_iter().enumerate() {
            let sid = sid_base + i; // gRPC ids follow the shred providers
            let reg = reg.clone();
            let cancel = cancel.clone();
            set.spawn(async move {
                supervise_source(sid, src, reg, cancel).await;
            });
        }
        while set.join_next().await.is_some() {}
    });
}

/// Keep one source connected across errors until cancelled.
async fn supervise_source(
    sid: usize,
    cfg: crate::config::GrpcSourceCfg,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
) {
    // Latch so the first connection failure is loud but reconnect churn is not.
    let announced = AtomicBool::new(false);
    while !cancel.is_cancelled() {
        match crate::grpc::run_source(sid, cfg.clone(), reg.clone(), cancel.clone()).await {
            Ok(()) => {}
            Err(e) => {
                if !announced.swap(true, Ordering::Relaxed) {
                    eprintln!("txn-compare: gRPC source `{}` error: {e:#}", cfg.name);
                }
            }
        }
        if cancel.is_cancelled() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}
