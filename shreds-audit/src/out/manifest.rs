use std::sync::atomic::Ordering;

use serde::Serialize;

use crate::{
    config::Config,
    leader::LeaderSchedule,
    pinger::NetMon,
    registry::Registry,
    rx::RxStats,
    sigreg::SourceKind,
    verify::{ProviderVerifyStats, VerifyStats},
};

use super::{hostname, now_unix_ns, WindowStats};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("GIT_COMMIT");
pub const SCHEMA_VERSION: u32 = 3;

#[derive(Serialize, Clone)]
pub struct Manifest {
    pub tool: &'static str,
    pub tool_version: &'static str,
    pub git_commit: &'static str,
    pub schema_version: u32,
    pub hostname: String,
    pub started_at_unix_ns: i64,
    pub ended_at_unix_ns: i64,
    pub clock_source: &'static str,
    pub timestamp_semantics: &'static str,
    pub providers: Vec<String>,
    pub rpc_url: String,
    pub leader_schedule_epoch: Option<u64>,
    pub min_slot: u64,
    pub max_slot: u64,
    pub rows_fec_sets: u64,
    pub rows_shreds: u64,
    pub rows_txns: u64,
    pub counters: Counters,
    pub provider_shreds: Vec<ProviderShreds>,
    pub provider_pings: Vec<ProviderPing>,
    pub leader_names: std::collections::HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_compare: Option<TxnCompareSummary>,
    pub notes: Vec<String>,
}

#[derive(Serialize, Clone, Default)]
pub struct TxnCompareSummary {
    pub distinct_signatures: u64,
    pub contested: u64,
    pub sources: Vec<TxnSource>,
    pub onchain_slots_checked: u64,
    pub onchain_slots_unavailable: u64,
    pub onchain_rpc_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onchain_last_error: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct TxnSource {
    pub name: String,
    pub kind: SourceKind,
    pub seen: u64,
    pub contested: u64,
    pub winrate: Option<f64>,
    pub behind_mean_us: Option<f64>,
    pub behind_p50_us: Option<f64>,
    pub behind_p90_us: Option<f64>,
    pub behind_p99_us: Option<f64>,

    pub onchain_slots_checked: u64,
    pub onchain_slots_absent: u64,
    pub onchain_txns: u64,
    pub onchain_missed: u64,
    pub onchain_corrupted: u64,
    pub onchain_duplicated: u64,
    pub onchain_bad: u64,
    pub onchain_bad_pct: Option<f64>,
}

#[derive(Serialize, Clone)]
pub struct ProviderShreds {
    pub provider: String,
    #[serde(flatten)]
    pub verify: ProviderVerifyStats,
    pub invalid_sig: u64,
    pub invalid_data: u64,
    pub invalid_unknown: u64,
}

#[derive(Serialize, Clone)]
pub struct ProviderPing {
    pub provider: String,
    pub ip: String,
    pub kind: SourceKind,
    pub source: &'static str,
    pub rtt_ms: Option<f64>,
    pub checked_at_unix_ns: Option<i64>,
}

#[derive(Serialize, Clone, Default)]
pub struct Counters {
    pub udp_received: u64,
    pub udp_unmatched: u64,
    pub udp_no_timestamp: u64,
    pub udp_channel_full: u64,
    pub udp_kernel_dropped: u64,
    pub udp_truncated: u64,
    pub shreds_parsed: u64,
    pub shreds_malformed: u64,
    pub shreds_unsupported_variant: u64,
    pub non_shred_pings: u64,
    pub shreds_wrong_version: u64,
    pub shreds_no_merkle_root: u64,
    pub shreds_no_leader: u64,
    pub shreds_sig_bad: u64,
    pub shreds_proof_stripped: u64,
    pub invalid_sig: u64,
    pub invalid_data: u64,
    pub invalid_unknown: u64,
    pub ed25519_verifies: u64,
    pub batch_fallbacks: u64,
    pub shreds_after_window: u64,
}

impl Counters {
    pub fn snapshot(
        rx: &RxStats,
        v: &VerifyStats,
        stats: &WindowStats,
        shreds_after_window: u64,
    ) -> Self {
        Self {
            udp_received: rx.received.load(Ordering::Relaxed),
            udp_unmatched: rx.unmatched.load(Ordering::Relaxed),
            udp_no_timestamp: rx.no_timestamp.load(Ordering::Relaxed),
            udp_channel_full: rx.channel_full.load(Ordering::Relaxed),
            udp_kernel_dropped: rx.kernel_dropped.load(Ordering::Relaxed),
            udp_truncated: rx.truncated.load(Ordering::Relaxed),
            shreds_parsed: v.parsed,
            shreds_malformed: v.malformed,
            shreds_unsupported_variant: v.unsupported_variant,
            non_shred_pings: v.non_shred_ping,
            shreds_wrong_version: v.wrong_version,
            shreds_no_merkle_root: v.no_merkle_root,
            shreds_no_leader: v.no_leader,
            shreds_sig_bad: v.sig_bad,
            shreds_proof_stripped: v.proof_stripped,
            invalid_sig: stats.invalid_sig,
            invalid_data: stats.invalid_data,
            invalid_unknown: stats.invalid_unknown,
            ed25519_verifies: v.ed25519_verifies,
            batch_fallbacks: v.batch_fallbacks,
            shreds_after_window,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_manifest(
    stats: &WindowStats,
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
    let counters = Counters::snapshot(rx_stats, vstats, stats, shreds_after_window);
    let mut notes = Vec::new();
    let no_ts = counters.udp_no_timestamp;
    if no_ts > 0 {
        notes.push(format!(
            "{no_ts} datagrams arrived with no SCM_TIMESTAMPNS control message and were discarded; \
             timings in this archive are from the remainder only"
        ));
    }
    let full = counters.udp_channel_full;
    if full > 0 {
        notes.push(format!(
            "{full} receive batches were dropped because the verify queue was full; \
             this machine could not keep up and coverage is incomplete"
        ));
    }
    let kernel_dropped = counters.udp_kernel_dropped;
    if kernel_dropped > 0 {
        notes.push(format!(
            "{kernel_dropped} datagrams were dropped by the KERNEL from this host's socket queue \
             (SO_RXQ_OVFL) — they never reached the tool. This is OUR loss, not a provider's: the \
             shreds in them are absent from the data and inflate `missed` for whichever provider \
             sent them. Do not read that as provider packet loss. Raise net.core.rmem_max and/or \
             reduce load on this host, then re-capture"
        ));
    }
    let truncated = counters.udp_truncated;
    if truncated > 0 {
        notes.push(format!(
            "{truncated} datagrams were larger than any Solana shred and were truncated by the \
             kernel; they were discarded rather than parsed. A provider sending these is probably \
             not sending one raw shred per datagram (batching, or an encapsulating header), and \
             none of its traffic of that shape is represented here"
        ));
    }
    if vstats.unsupported_variant > 0 {
        notes.push(format!(
            "{} shreds used a shred variant this build cannot parse (legacy, or newer than this \
             binary). They are counted here and excluded from every verdict — they are NOT counted \
             as invalid. If this number is large, this tool is out of date, not your provider",
            vstats.unsupported_variant
        ));
    }
    let unmatched = counters.udp_unmatched;
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
    if stats.invalid_data > 0 {
        notes.push(format!(
            "{} shreds carried block data that differs from the leader-signed copy of the same \
             shred. This is NOT a broken merkle proof over genuine data — the content itself is \
             not what the leader signed. Treat it as a substitution until proven otherwise",
            stats.invalid_data
        ));
    }
    if stats.invalid_sig > 0 {
        notes.push(format!(
            "{} shreds failed verification but carry the leader's genuine block data — their \
             merkle proof does not reconstruct the signed root. The data is authentic; the proof \
             of it is not, so the shred cannot be authenticated and agave will reject it",
            stats.invalid_sig
        ));
    }
    if stats.invalid_unknown > 0 {
        notes.push(format!(
            "{} shreds failed verification and no provider delivered a leader-authenticated copy \
             of the same shred, so they could not be classified as bad-signature or bad-data. \
             They are counted only under `invalid_unknown`, never folded into either",
            stats.invalid_unknown
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
    if stats.rows_txns > 0 {
        notes.push(
            "transactions.parquet carries `server_created_at_ns`, the only timestamp in this \
             archive taken off another machine's clock. Everything else here is one host's \
             CLOCK_REALTIME, where a difference is an exact subtraction; a difference against \
             this column also contains the sending server's clock offset, which no part of this \
             tool measures or corrects. Treat it as the source's own claim about when it had the \
             transaction, not as a measured network latency"
                .to_string(),
        );
    }

    Manifest {
        tool: "shred-audit",
        tool_version: VERSION,
        git_commit: GIT_COMMIT,
        schema_version: SCHEMA_VERSION,
        hostname: hostname(),
        started_at_unix_ns: started_at,
        ended_at_unix_ns: now_unix_ns(),
        clock_source: "SO_TIMESTAMPNS (kernel, CLOCK_REALTIME, stamped at driver handoff)",
        timestamp_semantics: "absolute unix nanoseconds; provider deltas are exact subtractions \
                              on a single host clock, no baseline provider involved",
        providers: registry.names().to_vec(),
        rpc_url: cfg.rpc_endpoint().map(|r| r.label()).unwrap_or_default(),
        leader_schedule_epoch: schedule.epoch(),
        min_slot: stats.min_slot_or_zero(),
        max_slot: stats.max_slot,
        rows_fec_sets: stats.rows_sets,
        rows_shreds: stats.rows_shreds,
        rows_txns: stats.rows_txns,
        provider_shreds: provider_shreds(registry, vstats, stats),
        provider_pings: netmon.provider_pings(cfg, registry),
        leader_names: schedule.leader_names(),
        txn_compare: txn.cloned(),
        counters,
        notes,
    }
}

fn provider_shreds(
    registry: &Registry,
    vstats: &VerifyStats,
    stats: &WindowStats,
) -> Vec<ProviderShreds> {
    registry
        .names()
        .iter()
        .enumerate()
        .map(|(id, name)| {
            let invalid = stats.providers.get(id).copied().unwrap_or_default();
            ProviderShreds {
                provider: name.clone(),
                verify: vstats.providers.get(id).copied().unwrap_or_default(),
                invalid_sig: invalid.invalid_sig,
                invalid_data: invalid.invalid_data,
                invalid_unknown: invalid.invalid_unknown,
            }
        })
        .collect()
}

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
    if cfg.onchain_rpc_endpoint().is_ok_and(|rpc| rpc.omits_votes()) {
        notes.push(
            "the onchain audit's RPC leaves vote transactions out of getBlock, so votes are \
             excluded on both sides: every `onchain_*` count covers non-vote transactions only"
                .to_string(),
        );
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
        cfg.onchain_rpc_endpoint()
            .map(|r| r.label())
            .unwrap_or_default(),
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
        let why = match worst.kind {
            SourceKind::GrpcDeshred => {
                "this is a pre-execution deshred feed: it reports transactions before execution, \
                 so one that never landed is expected here and is not fabrication"
            }
            SourceKind::Shred => {
                "this is the local shred reconstruction, which is also pre-execution — a \
                 transaction from a fork that lost looks exactly like this"
            }
            SourceKind::Grpc => {
                "this is a post-execution subscription, where the benign explanation is a \
                 `processed` commitment delivering from a fork that lost; at `confirmed` or \
                 `finalized` it deserves a closer look"
            }
        };
        notes.push(format!(
            "source `{}` (kind `{}`) delivered {} of {} transactions the sampled blocks do not \
             contain — {}. Read it next to `onchain_missed` before treating it as fabrication",
            worst.name,
            worst.kind.label(),
            worst.onchain_corrupted,
            worst.onchain_txns,
            why,
        ));
    }
    notes
}
