//! Cross-source transaction correlation, keyed by transaction signature.
//!
//! A transaction's first signature (`signatures[0]`, 64 bytes) is the natural
//! join key across paths. Per signature we keep one first-seen timestamp slot per
//! source, and derive which source delivered first and by how much.
//!
//! Every timestamp is an absolute CLOCK_REALTIME nanosecond value on this one
//! host, so a shred-vs-gRPC delta is an exact subtraction. One asymmetry: a shred
//! arrival is stamped by the kernel at driver handoff, while a gRPC arrival is
//! stamped in userspace only after decrypt+decode, so the gRPC side carries a
//! little extra local processing latency — a property of where each path can be
//! measured, not of the network. Called out in the report.
//!
//! Memory is bounded for a run of any length. In-flight signatures live in
//! `events` only until their slot falls `EVICT_MARGIN_SLOTS` behind the highest
//! slot seen; at that point the row is *finalized* — folded into per-source
//! running counters and retired. The whole-run winrate/seen/contested counts and
//! the mean are therefore exact, while the behind-distribution percentiles are
//! taken over a bounded ring of the most recent samples. Nothing grows without
//! limit, matching the O(providers) discipline of the FEC-set aggregator.

use std::collections::VecDeque;

use ahash::AHashMap;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SourceKind {
    /// Transactions reconstructed from the shred (UDP) stream.
    Shred,
    /// Transactions arriving over a Geyser gRPC subscription.
    Grpc,
}

/// Most recent behind-samples kept per source for percentile estimation. Older
/// samples are dropped once this many accumulate; the mean and all counts stay
/// exact over the whole run, only the percentiles are over this recent window.
const BEHIND_SAMPLE_CAP: usize = 65_536;

/// A signature is finalized (scored and retired) once its slot falls this many
/// slots behind the highest slot observed on any source, so a slow source still
/// lands before the row is retired. ~64 slots ≈ 25 s, well past the shred
/// reconstruction settle window.
const EVICT_MARGIN_SLOTS: u64 = 64;

/// Per-signature record held while the race is still in flight: the slot it
/// belongs to and the first-seen unix-ns per source (indexed by source id).
struct SigRow {
    slot: u64,
    ts: Vec<Option<i64>>,
}

pub struct SigRegistry {
    names: Vec<String>,
    kinds: Vec<SourceKind>,
    /// Signatures whose race is still in flight, retired on finalize.
    events: AHashMap<[u8; 64], SigRow>,
    /// Highest slot seen on any source; drives the finalize floor.
    high_slot: u64,

    // Whole-run running totals, folded in at finalize (exact).
    distinct_total: u64,
    contested_total: u64,
    /// Distinct signatures observed per source.
    seen: Vec<u64>,
    /// Contested signatures (seen by ≥2 sources) this source was present for.
    contested: Vec<u64>,
    /// Contested signatures this source delivered first (ties all count).
    wins: Vec<u64>,
    /// Exact mean numerator/denominator of ns-behind-earliest, per source.
    behind_sum_ns: Vec<i128>,
    behind_n: Vec<u64>,
    /// Bounded ring of recent ns-behind samples per source, for percentiles.
    behind_ring: Vec<VecDeque<i64>>,
}

impl SigRegistry {
    pub fn new(names: Vec<String>, kinds: Vec<SourceKind>) -> Self {
        assert_eq!(names.len(), kinds.len());
        let n = names.len();
        Self {
            names,
            kinds,
            events: AHashMap::new(),
            high_slot: 0,
            distinct_total: 0,
            contested_total: 0,
            seen: vec![0; n],
            contested: vec![0; n],
            wins: vec![0; n],
            behind_sum_ns: vec![0; n],
            behind_n: vec![0; n],
            behind_ring: vec![VecDeque::new(); n],
        }
    }

    pub fn name(&self, sid: usize) -> &str {
        &self.names[sid]
    }

    pub fn kind(&self, sid: usize) -> SourceKind {
        self.kinds[sid]
    }

    /// Distinct signatures observed across all sources over the whole run.
    pub fn distinct_signatures(&self) -> u64 {
        self.distinct_total
    }

    /// Contested signatures (seen by at least two sources) finalized so far.
    pub fn contested_signatures(&self) -> u64 {
        self.contested_total
    }

    /// Record that `sid` first saw `sig` at `ns`. Only the earliest arrival per
    /// (signature, source) is kept: a later re-delivery of the same signature on
    /// the same source must not move the timestamp or it would erase the very
    /// race we are trying to time.
    pub fn record_first(&mut self, sid: usize, sig: [u8; 64], ns: i64, slot: u64) {
        if slot > self.high_slot {
            self.high_slot = slot;
        }
        let n = self.names.len();
        if !self.events.contains_key(&sig) {
            self.distinct_total += 1;
        }
        let row = self.events.entry(sig).or_insert_with(|| SigRow {
            slot,
            ts: vec![None; n],
        });
        // Keep the first non-zero slot learned (a source may report 0 if unavailable).
        if row.slot == 0 && slot != 0 {
            row.slot = slot;
        }
        if row.ts[sid].is_none() {
            row.ts[sid] = Some(ns);
            self.seen[sid] += 1;
        }
    }

    /// Retire settled signatures into the running totals to keep `events` bounded.
    /// With `force`, retire everything (used for the final snapshot); otherwise
    /// only rows whose slot has fallen `EVICT_MARGIN_SLOTS` behind the tip.
    pub fn finalize(&mut self, force: bool) {
        let floor = self.high_slot.saturating_sub(EVICT_MARGIN_SLOTS);
        let retire: Vec<[u8; 64]> = self
            .events
            .iter()
            .filter(|(_, row)| force || (row.slot != 0 && row.slot < floor))
            .map(|(&k, _)| k)
            .collect();
        for k in retire {
            if let Some(row) = self.events.remove(&k) {
                self.fold(&row);
            }
        }
    }

    /// Fold one retired signature into the per-source running totals.
    fn fold(&mut self, row: &SigRow) {
        let n = self.names.len();
        let present: Vec<usize> = (0..n).filter(|&i| row.ts[i].is_some()).collect();
        if present.len() < 2 {
            return; // a race needs at least two sources
        }
        self.contested_total += 1;
        let min = present.iter().map(|&i| row.ts[i].unwrap()).min().unwrap();
        for &i in &present {
            let ts = row.ts[i].unwrap();
            self.contested[i] += 1;
            let behind = ts - min; // ns behind the earliest source (>= 0)
            self.behind_sum_ns[i] += behind as i128;
            self.behind_n[i] += 1;
            let ring = &mut self.behind_ring[i];
            if ring.len() == BEHIND_SAMPLE_CAP {
                ring.pop_front();
            }
            ring.push_back(behind);
            if ts == min {
                self.wins[i] += 1; // all sources tied at the earliest ns win
            }
        }
    }

    /// Per-source raw totals plus a clone of the recent behind-sample ring. The
    /// clone lets the caller compute percentiles (a sort) *outside* the registry
    /// lock, so the hot `record_first` path is never blocked on that work.
    pub fn export(&self) -> Vec<SourceRaw> {
        (0..self.names.len())
            .map(|i| SourceRaw {
                seen: self.seen[i],
                contested: self.contested[i],
                wins: self.wins[i],
                mean_us: (self.behind_n[i] > 0).then(|| {
                    self.behind_sum_ns[i] as f64 / self.behind_n[i] as f64 / 1000.0
                }),
                behind_ns: self.behind_ring[i].iter().copied().collect(),
            })
            .collect()
    }
}

/// One source's whole-run totals for the snapshot. `behind_ns` is a copy of the
/// recent behind-sample ring; run it through [`percentiles_us`] to summarise.
#[derive(Clone, Debug, Default)]
pub struct SourceRaw {
    pub seen: u64,
    pub contested: u64,
    pub wins: u64,
    pub mean_us: Option<f64>,
    pub behind_ns: Vec<i64>,
}

/// p50 / p90 / p99 of a sample set, in microseconds. `None` when empty.
#[derive(Clone, Copy, Debug, Default)]
pub struct BehindPct {
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p99: Option<f64>,
}

/// Nearest-rank percentiles of `samples_ns` (mutated in place: sorted). Kept out
/// of the registry so the sort runs without the lock held.
pub fn percentiles_us(samples_ns: &mut [i64]) -> BehindPct {
    if samples_ns.is_empty() {
        return BehindPct::default();
    }
    samples_ns.sort_unstable();
    let n = samples_ns.len();
    let pick = |p: f64| -> f64 {
        let idx = ((n as f64 - 1.0) * p) as usize;
        samples_ns[idx] as f64 / 1000.0
    };
    BehindPct {
        p50: Some(pick(0.5)),
        p90: Some(pick(0.9)),
        p99: Some(pick(0.99)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(b: u8) -> [u8; 64] {
        let mut s = [0u8; 64];
        s[0] = b;
        s
    }

    fn reg() -> SigRegistry {
        SigRegistry::new(
            vec!["shreds".into(), "grpc-a".into()],
            vec![SourceKind::Shred, SourceKind::Grpc],
        )
    }

    #[test]
    fn first_seen_wins_and_dedupes() {
        let mut r = reg();
        r.record_first(0, sig(1), 1_000, 42);
        // a later re-delivery of the same signature on the same source is ignored
        r.record_first(0, sig(1), 5_000, 42);
        r.finalize(true);
        let raw = r.export();
        assert_eq!(raw[0].seen, 1);
        // only one source saw it -> not contested
        assert_eq!(r.contested_signatures(), 0);
        assert_eq!(raw[0].contested, 0);
    }

    #[test]
    fn shred_ahead_of_grpc_wins_and_measures_behind() {
        let mut r = reg();
        r.record_first(0, sig(1), 1_000, 42); // shred at t=1000
        r.record_first(1, sig(1), 3_000, 42); // grpc  at t=3000
        r.finalize(true);
        let raw = r.export();
        assert_eq!(r.contested_signatures(), 1);
        assert_eq!(raw[0].wins, 1, "shred delivered first");
        assert_eq!(raw[1].wins, 0);
        // grpc is 2us behind the earliest (shred), shred is 0 behind
        let mut b0 = raw[0].behind_ns.clone();
        let mut b1 = raw[1].behind_ns.clone();
        assert_eq!(percentiles_us(&mut b0).p50, Some(0.0));
        assert_eq!(percentiles_us(&mut b1).p50, Some(2.0));
    }

    #[test]
    fn grpc_ahead_wins() {
        let mut r = reg();
        r.record_first(1, sig(2), 2_000, 42); // grpc first
        r.record_first(0, sig(2), 9_000, 42); // shred later
        r.finalize(true);
        let raw = r.export();
        assert_eq!(raw[1].wins, 1);
        assert_eq!(raw[0].wins, 0);
        let mut b0 = raw[0].behind_ns.clone();
        assert_eq!(percentiles_us(&mut b0).p50, Some(7.0)); // 9000-2000 = 7us
    }

    #[test]
    fn slot_floor_eviction_keeps_events_bounded() {
        let mut r = reg();
        // an old contested signature at slot 10
        r.record_first(0, sig(1), 1_000, 10);
        r.record_first(1, sig(1), 2_000, 10);
        // tip advances well past the eviction margin
        r.record_first(0, sig(2), 3_000, 10 + EVICT_MARGIN_SLOTS + 5);
        r.finalize(false);
        // slot-10 row is finalized and retired; the fresh row stays in flight
        assert_eq!(r.contested_signatures(), 1);
        assert_eq!(r.export()[0].wins, 1);
    }
}
