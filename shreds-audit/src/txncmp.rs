use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use bytes::Bytes;
use crossbeam_channel::Sender;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, GrpcMode, GrpcSourceCfg};
use crate::deshred::{Deshredder, ShredInput};
use crate::filters::{
    audit::{FilterAudit, WindowInfo},
    bundles::Bundle,
    FilterAuditor, Rotation,
};
use crate::grpc::{run_source, Subscription};
use crate::out::{TxnCompareSummary, TxnSource};
use crate::sigreg::{SigRegistry, SourceKind, TxnRow};
use crate::verification::onchain_signatures::{OnchainAudit, OnchainVerifier};

pub struct TxnCompare {
    reg: Arc<Mutex<SigRegistry>>,
    feed: Option<Sender<ShredInput>>,
    deshred_handle: Option<JoinHandle<()>>,
    async_handle: Option<JoinHandle<()>>,
    onchain: Option<OnchainVerifier>,
    cancel: CancellationToken,
    labels: Vec<(String, SourceKind)>,
    filter_audit: Option<FilterAuditor>,
}

impl TxnCompare {
    /// Returns `None` when no gRPC sources are configured.
    pub fn start(
        cfg: &Config,
        dump_txns: bool,
        filter_db: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        if cfg.grpc_sources.is_empty() {
            return Ok(None);
        }

        // Source ids: shred providers first (sid = provider id), then gRPC sources.
        let n_providers = cfg.providers.len();
        let labels: Vec<(String, SourceKind)> = cfg
            .providers
            .iter()
            .map(|p| (p.name.clone(), SourceKind::Shred))
            .chain(
                cfg.grpc_sources
                    .iter()
                    .map(|g| (g.name.clone(), SourceKind::from(g.mode))),
            )
            .collect();
        let n_sources = labels.len();
        let (names, kinds) = labels.iter().cloned().unzip();
        let mut reg = SigRegistry::new(names, kinds);
        if dump_txns {
            reg.enable_txn_rows();
        }
        for (i, g) in cfg.grpc_sources.iter().enumerate() {
            if cfg.rotating(g) {
                reg.set_rotating(n_providers + i);
            }
        }
        let reg = Arc::new(Mutex::new(reg));
        let cancel = CancellationToken::new();
        let filter_audit = FilterAuditor::start(cfg, reg.clone(), filter_db)?;

        // Started before anything records, so the onchain index sees the opening slots.
        let onchain_rpc = cfg.onchain_rpc_endpoint()?;
        let onchain = cfg.onchain_verify.then(|| {
            OnchainVerifier::start(
                onchain_rpc.clone(),
                cfg.onchain_lag_slots,
                cfg.onchain_sample_secs,
                n_sources,
                reg.clone(),
                cancel.clone(),
            )
        });

        // Bounded: a stalled deshredder drops feed instead of growing a backlog.
        let (feed, feed_rx) = crossbeam_channel::bounded::<ShredInput>(131_072);
        let deshredder = Deshredder::new(reg.clone());
        let deshred_handle = std::thread::Builder::new()
            .name("deshred".into())
            .spawn(move || deshredder.run(feed_rx))
            .ok();

        let sources: Vec<SourcePlan> = cfg
            .grpc_sources
            .iter()
            .enumerate()
            .map(|(i, g)| SourcePlan {
                sid: n_providers + i,
                cfg: g.clone(),
                rotation: filter_audit.as_ref().filter(|_| cfg.rotating(g)).map(|fa| {
                    (
                        fa.audit.clone(),
                        FilterAuditor::rotation(cfg, g.mode, (n_providers + i) as u64 + 1),
                    )
                }),
            })
            .collect();
        let async_reg = reg.clone();
        let async_cancel = cancel.clone();
        let async_handle = std::thread::Builder::new()
            .name("txn-async-sources".into())
            .spawn(move || run_async_runtime(sources, async_reg, async_cancel))
            .ok();

        let transaction_sources = cfg
            .grpc_sources
            .iter()
            .filter(|g| g.mode == GrpcMode::Transactions)
            .count();
        let deshred_sources = cfg.grpc_sources.len() - transaction_sources;
        eprintln!(
            "txn-compare: reconstructing transactions per shred provider (with Reed-Solomon \
             recovery) and subscribing to {} transaction + {} deshred gRPC source(s); racing \
             every source by transaction signature",
            transaction_sources, deshred_sources
        );
        if onchain.is_some() {
            eprintln!(
                "txn-compare: auditing every source against the chain — sampling one slot every \
                 {}s at {} slots behind the tip via getBlock on {}",
                cfg.onchain_sample_secs,
                cfg.onchain_lag_slots,
                onchain_rpc.label()
            );
        }

        Ok(Some(Self {
            reg,
            feed: Some(feed),
            deshred_handle,
            async_handle,
            onchain,
            cancel,
            labels,
            filter_audit,
        }))
    }

    pub fn harvest(&self) -> Vec<TxnRow> {
        self.reg.lock().unwrap().drain_rows()
    }

    pub fn labels(&self) -> &[(String, SourceKind)] {
        &self.labels
    }

    pub fn snapshot(&self) -> TxnCompareSummary {
        build_snapshot(&self.reg, self.onchain_audit(), false)
    }

    /// Finalizes every in-flight signature, including those still inside the eviction margin.
    pub fn final_snapshot(&self) -> TxnCompareSummary {
        build_snapshot(&self.reg, self.onchain_audit(), true)
    }

    fn onchain_audit(&self) -> Option<OnchainAudit> {
        self.onchain.as_ref().map(|v| v.snapshot())
    }

    pub fn feed(&self, rx_unix_ns: i64, provider: u16, data: &[u8]) {
        if !crate::verify::is_shred_payload(data) {
            return;
        }
        if let Some(feed) = &self.feed {
            let _ = feed.try_send(ShredInput {
                rx_unix_ns,
                provider,
                data: Bytes::copy_from_slice(data),
            });
        }
    }

    pub fn shutdown(&mut self) {
        self.cancel.cancel();
        drop(self.feed.take());
        if let Some(h) = self.deshred_handle.take() {
            let _ = h.join();
        }
        if let Some(h) = self.async_handle.take() {
            let _ = h.join();
        }
        if let Some(fa) = self.filter_audit.take() {
            fa.finish();
        }
    }

    pub fn finish(mut self) {
        self.shutdown();
        let audit = self.onchain.take().map(|v| {
            let audit = v.snapshot();
            v.finish();
            audit
        });
        report(&self.reg, audit);
    }
}

fn build_snapshot(
    reg: &Mutex<SigRegistry>,
    audit: Option<OnchainAudit>,
    force: bool,
) -> TxnCompareSummary {
    let audit = audit.unwrap_or_default();
    let mut reg = reg.lock().unwrap();
    reg.finalize(force);
    let sources = reg
        .export()
        .into_iter()
        .enumerate()
        .map(|(sid, raw)| {
            let oc = audit.sources.get(sid).copied().unwrap_or_default();
            TxnSource {
                name: reg.name(sid).to_string(),
                kind: reg.kind(sid),
                seen: raw.seen,
                contested: raw.contested,
                winrate: (raw.contested > 0).then(|| raw.wins as f64 / raw.contested as f64),
                behind_mean_us: raw.mean_us,
                behind_p50_us: raw.p50_us,
                behind_p90_us: raw.p90_us,
                behind_p99_us: raw.p99_us,
                onchain_slots_checked: oc.slots_checked,
                onchain_slots_absent: oc.slots_absent,
                onchain_txns: oc.onchain_txns,
                onchain_missed: oc.missed,
                onchain_corrupted: oc.corrupted,
                onchain_duplicated: oc.duplicated,
                onchain_bad: oc.bad(),
                onchain_bad_pct: oc.bad_fraction(),
            }
        })
        .collect();
    TxnCompareSummary {
        distinct_signatures: reg.distinct_signatures(),
        contested: reg.contested_signatures(),
        sources,
        onchain_slots_checked: audit.slots_checked,
        onchain_slots_unavailable: audit.slots_unavailable,
        onchain_rpc_errors: audit.rpc_errors,
        onchain_last_error: audit.last_error,
    }
}

fn report(reg: &Mutex<SigRegistry>, audit: Option<OnchainAudit>) {
    let snap = build_snapshot(reg, audit, true);
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

    if snap.onchain_slots_checked == 0 {
        return;
    }
    eprintln!(
        "\nonchain audit: {} slots sampled ({} skipped by the cluster, {} rpc errors)",
        snap.onchain_slots_checked, snap.onchain_slots_unavailable, snap.onchain_rpc_errors
    );
    for s in &snap.sources {
        if s.onchain_slots_checked == 0 && s.onchain_slots_absent == 0 {
            continue;
        }
        eprintln!(
            "  {:<20} bad={} ({} of {} txns: missed {} corrupted {} duplicated {}) \
             over {} slots, absent for {}",
            s.name,
            s.onchain_bad_pct
                .map(|p| format!("{:.3}%", p * 100.0))
                .unwrap_or_else(|| "—".into()),
            s.onchain_bad,
            s.onchain_txns,
            s.onchain_missed,
            s.onchain_corrupted,
            s.onchain_duplicated,
            s.onchain_slots_checked,
            s.onchain_slots_absent,
        );
    }
}

struct SourcePlan {
    sid: usize,
    cfg: GrpcSourceCfg,
    rotation: Option<(Arc<FilterAudit>, Rotation)>,
}

fn run_async_runtime(
    sources: Vec<SourcePlan>,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
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
        for plan in sources {
            set.spawn(supervise_grpc_source(plan, reg.clone(), cancel.clone()));
        }
        while set.join_next().await.is_some() {}
    });
}

async fn supervise_grpc_source(
    plan: SourcePlan,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
) {
    let SourcePlan {
        sid,
        cfg,
        mut rotation,
    } = plan;
    // Only the first failure is logged, so reconnect churn stays quiet.
    let mut announced = false;
    let mut connection_id: u32 = 0;
    while !cancel.is_cancelled() {
        let sub = match rotation.as_mut() {
            Some((audit, rotation)) => {
                let (bundle, dur) = rotation.next();
                let key = (sid, connection_id);
                audit.open(
                    key,
                    WindowInfo {
                        source: cfg.name.clone(),
                        kind: SourceKind::from(cfg.mode),
                        commitment: match cfg.mode {
                            GrpcMode::Transactions => Some(cfg.effective_commitment().to_string()),
                            GrpcMode::Deshred => None,
                        },
                        bundle: bundle.clone(),
                    },
                );
                Subscription {
                    bundle,
                    window: Some((audit.clone(), key)),
                    deadline: Some(tokio::time::Instant::now() + dur),
                }
            }
            None => Subscription {
                bundle: Bundle::all(),
                window: None,
                deadline: None,
            },
        };
        let window = sub.window.clone();
        let result = run_source(sid, cfg.clone(), sub, reg.clone(), cancel.clone(), connection_id).await;
        if let Some((audit, key)) = window {
            let reason = match &result {
                Ok(end) => end.label().to_string(),
                Err(e) => format!("error: {e:#}"),
            };
            audit.close(key, reason);
        }
        let failed = result.is_err();
        if let Err(e) = result {
            if !announced {
                announced = true;
                eprintln!("txn-compare: gRPC source `{}` error: {e:#}", cfg.name);
            }
        }
        connection_id += 1;
        if cancel.is_cancelled() {
            break;
        }
        if failed || rotation.is_none() {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_labels_every_source_kind_distinctly() {
        let names = vec!["s1".into(), "g1".into(), "d1".into()];
        let kinds = vec![SourceKind::Shred, SourceKind::Grpc, SourceKind::GrpcDeshred];
        let reg = Arc::new(Mutex::new(SigRegistry::new(names, kinds)));

        let snap = build_snapshot(&reg, None, false);
        assert_eq!(snap.sources.len(), 3);
        assert_eq!(snap.sources[0].kind, SourceKind::Shred);
        assert_eq!(snap.sources[1].kind, SourceKind::Grpc);
        assert_eq!(snap.sources[2].kind, SourceKind::GrpcDeshred);
    }

    #[test]
    fn source_kind_serializes_to_its_label() {
        let names = vec!["s1".into(), "g1".into(), "d1".into()];
        let kinds = vec![SourceKind::Shred, SourceKind::Grpc, SourceKind::GrpcDeshred];
        let reg = Arc::new(Mutex::new(SigRegistry::new(names, kinds)));

        let snap = build_snapshot(&reg, None, false);
        let json = serde_json::to_value(&snap).unwrap();
        let kinds: Vec<&str> = json["sources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["shreds", "grpc", "grpc-deshred"]);
    }
}
