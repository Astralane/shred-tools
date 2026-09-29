use ahash::AHashMap;
use hdrhistogram::Histogram;

use crate::verification::onchain_signatures::{SlotSigIndex, SourceSlot};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SourceKind {
    Shred,
    Grpc,
    GrpcDeshred,
}

impl SourceKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Shred => "shreds",
            Self::Grpc => "grpc",
            Self::GrpcDeshred => "grpc-deshred",
        }
    }
}

impl serde::Serialize for SourceKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.label())
    }
}

impl From<crate::config::GrpcMode> for SourceKind {
    fn from(mode: crate::config::GrpcMode) -> Self {
        match mode {
            crate::config::GrpcMode::Transactions => Self::Grpc,
            crate::config::GrpcMode::Deshred => Self::GrpcDeshred,
        }
    }
}

pub const VOTE_PROGRAM_ID: [u8; 32] = [
    7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94, 115, 209,
    16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
];

pub fn is_simple_vote(
    signatures: usize,
    legacy: bool,
    instructions: usize,
    program_id: Option<&[u8]>,
) -> bool {
    signatures < 3 && legacy && instructions == 1 && program_id == Some(&VOTE_PROGRAM_ID[..])
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TxnMeta {
    pub server_created_at_ns: Option<i64>,
    pub is_vote: Option<bool>,
    pub message_size: Option<u32>,
    pub connection_id: Option<u32>,
}

pub struct TxnRow {
    pub sig: [u8; 64],
    pub slot: u64,
    pub sid: u16,
    pub first_rx_unix_ns: i64,
    pub duplicate_count: u32,
    pub meta: TxnMeta,
}

/// Rows are retired this many slots behind the tip, so a slow source still lands first (~25 s).
const EVICT_MARGIN_SLOTS: u64 = 64;

const WINDOW_RESERVOIR: usize = 50_000;

#[derive(Clone, Copy)]
struct Seen {
    ns: i64,
    dups: u32,
    meta: TxnMeta,
}

struct SigRow {
    slot: u64,
    ts: Vec<Option<Seen>>,
}

pub struct SigRegistry {
    names: Vec<String>,
    kinds: Vec<SourceKind>,
    events: AHashMap<[u8; 64], SigRow>,
    high_slot: u64,
    distinct_total: u64,
    contested_total: u64,
    seen: Vec<u64>,
    contested: Vec<u64>,
    wins: Vec<u64>,
    behind_sum_ns: Vec<i128>,
    behind_n: Vec<u64>,
    behind_histogram: Vec<Histogram<u64>>,
    onchain: Option<SlotSigIndex>,
    rows: Vec<TxnRow>,
    collect_rows: bool,
    rotating: Vec<bool>,
    window_latency: AHashMap<(usize, u32), WindowLatency>,
    finalized_floor: u64,
}

fn behind_histogram() -> Histogram<u64> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    hist.auto(true);
    hist
}

pub struct WindowLatency {
    pub contested: u64,
    pub wins: u64,
    pub behind_us: Histogram<u64>,
    pub vs_shred_us: Reservoir,
}

impl WindowLatency {
    fn new() -> Self {
        Self {
            contested: 0,
            wins: 0,
            behind_us: behind_histogram(),
            vs_shred_us: Reservoir::new(WINDOW_RESERVOIR),
        }
    }
}

pub struct Reservoir {
    cap: usize,
    seen: u64,
    values: Vec<i64>,
    rng: u64,
}

impl Reservoir {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            seen: 0,
            values: Vec::new(),
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    pub fn push(&mut self, v: i64) {
        self.seen += 1;
        if self.values.len() < self.cap {
            self.values.push(v);
            return;
        }
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let j = self.rng % self.seen;
        if (j as usize) < self.cap {
            self.values[j as usize] = v;
        }
    }

    pub fn count(&self) -> u64 {
        self.seen
    }

    pub fn percentiles(&mut self, qs: &[f64]) -> Vec<Option<f64>> {
        if self.values.is_empty() {
            return vec![None; qs.len()];
        }
        self.values.sort_unstable();
        let n = self.values.len();
        qs.iter()
            .map(|q| {
                let i = ((q * n as f64).ceil() as usize).clamp(1, n) - 1;
                Some(self.values[i] as f64)
            })
            .collect()
    }
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
            behind_histogram: (0..n).map(|_| behind_histogram()).collect(),
            onchain: None,
            rows: Vec::new(),
            collect_rows: false,
            rotating: vec![false; n],
            window_latency: AHashMap::new(),
            finalized_floor: 0,
        }
    }

    pub fn set_rotating(&mut self, sid: usize) {
        self.rotating[sid] = true;
    }

    pub fn is_rotating(&self, sid: usize) -> bool {
        self.rotating[sid]
    }

    pub fn high_slot(&self) -> u64 {
        self.high_slot
    }

    pub fn finalized_floor(&self) -> u64 {
        self.finalized_floor
    }

    pub fn take_window_latency(&mut self, sid: usize, connection_id: u32) -> Option<WindowLatency> {
        self.window_latency.remove(&(sid, connection_id))
    }

    pub fn name(&self, sid: usize) -> &str {
        &self.names[sid]
    }

    pub fn kind(&self, sid: usize) -> SourceKind {
        self.kinds[sid]
    }

    pub fn distinct_signatures(&self) -> u64 {
        self.distinct_total
    }

    pub fn contested_signatures(&self) -> u64 {
        self.contested_total
    }

    pub fn record_first(&mut self, sid: usize, sig: [u8; 64], ns: i64, slot: u64, meta: TxnMeta) {
        self.high_slot = self.high_slot.max(slot);
        // Before the dedupe: the onchain audit must see re-deliveries the race ignores.
        if let Some(index) = &mut self.onchain {
            if !self.rotating[sid] {
                index.record(sid, slot, &sig);
            }
        }
        let n = self.names.len();
        let row = self.events.entry(sig).or_insert_with(|| {
            self.distinct_total += 1;
            SigRow {
                slot,
                ts: vec![None; n],
            }
        });
        // A source may report slot 0 when it does not know the slot.
        if row.slot == 0 {
            row.slot = slot;
        }
        match &mut row.ts[sid] {
            Some(seen) => seen.dups += 1,
            empty => {
                *empty = Some(Seen { ns, dups: 0, meta });
                self.seen[sid] += 1;
            }
        }
    }

    pub fn enable_txn_rows(&mut self) {
        self.collect_rows = true;
    }

    pub fn drain_rows(&mut self) -> Vec<TxnRow> {
        std::mem::take(&mut self.rows)
    }

    pub fn enable_onchain_index(&mut self, retain_slots: u64) {
        self.onchain = Some(SlotSigIndex::new(self.names.len(), retain_slots));
    }

    pub fn onchain_tip(&self) -> u64 {
        self.onchain.as_ref().map_or(0, |index| index.tip())
    }

    pub fn take_onchain_slot(&mut self, slot: u64) -> Option<Vec<SourceSlot>> {
        self.onchain.as_mut()?.take(slot)
    }

    /// Retire rows whose slot fell `EVICT_MARGIN_SLOTS` behind the tip (all rows with `force`).
    pub fn finalize(&mut self, force: bool) {
        let floor = self.high_slot.saturating_sub(EVICT_MARGIN_SLOTS);
        self.finalized_floor = if force { u64::MAX } else { floor };
        let retire: Vec<[u8; 64]> = self
            .events
            .iter()
            .filter(|(_, row)| force || (row.slot != 0 && row.slot < floor))
            .map(|(&sig, _)| sig)
            .collect();
        for sig in retire {
            let row = self.events.remove(&sig).unwrap();
            if self.collect_rows {
                self.emit_rows(&sig, &row);
            }
            self.fold(&row);
        }
    }

    fn emit_rows(&mut self, sig: &[u8; 64], row: &SigRow) {
        for (sid, seen) in row.ts.iter().enumerate() {
            let Some(seen) = seen else { continue };
            self.rows.push(TxnRow {
                sig: *sig,
                slot: row.slot,
                sid: sid as u16,
                first_rx_unix_ns: seen.ns,
                duplicate_count: seen.dups,
                meta: seen.meta,
            });
        }
    }

    fn fold(&mut self, row: &SigRow) {
        let present: Vec<(usize, Seen)> = row
            .ts
            .iter()
            .enumerate()
            .filter_map(|(i, seen)| seen.map(|seen| (i, seen)))
            .collect();
        if present.len() < 2 {
            return;
        }
        self.contested_total += 1;
        let min = present.iter().map(|(_, seen)| seen.ns).min().unwrap();
        let min_shred = present
            .iter()
            .filter(|(i, _)| self.kinds[*i] == SourceKind::Shred)
            .map(|(_, seen)| seen.ns)
            .min();
        for (i, seen) in present {
            let behind = seen.ns - min;
            let behind_us = (behind / 1000) as u64;
            let is_win = seen.ns == min;
            self.contested[i] += 1;
            self.behind_sum_ns[i] += behind as i128;
            self.behind_n[i] += 1;
            let _ = self.behind_histogram[i].record(behind_us);
            if is_win {
                self.wins[i] += 1;
            }
            if self.rotating[i] {
                let conn = seen.meta.connection_id.unwrap_or(0);
                let w = self
                    .window_latency
                    .entry((i, conn))
                    .or_insert_with(WindowLatency::new);
                w.contested += 1;
                w.wins += is_win as u64;
                let _ = w.behind_us.record(behind_us);
                if let Some(shred) = min_shred {
                    w.vs_shred_us.push((seen.ns - shred) / 1000);
                }
            }
        }
    }

    pub fn export(&self) -> Vec<SourceRaw> {
        (0..self.names.len())
            .map(|i| {
                let scored = self.behind_n[i] > 0;
                let hist = &self.behind_histogram[i];
                let quantile = |q| scored.then(|| hist.value_at_quantile(q) as f64);
                SourceRaw {
                    seen: self.seen[i],
                    contested: self.contested[i],
                    wins: self.wins[i],
                    mean_us: scored
                        .then(|| self.behind_sum_ns[i] as f64 / self.behind_n[i] as f64 / 1000.0),
                    p50_us: quantile(0.5),
                    p90_us: quantile(0.9),
                    p99_us: quantile(0.99),
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct SourceRaw {
    pub seen: u64,
    pub contested: u64,
    pub wins: u64,
    pub mean_us: Option<f64>,
    pub p50_us: Option<f64>,
    pub p90_us: Option<f64>,
    pub p99_us: Option<f64>,
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
    fn simple_vote_rule_holds_at_its_boundaries() {
        let vote = Some(&VOTE_PROGRAM_ID[..]);
        let other = [9u8; 32];
        assert!(is_simple_vote(1, true, 1, vote));
        assert!(is_simple_vote(2, true, 1, vote));
        assert!(!is_simple_vote(3, true, 1, vote), "three signers is not a simple vote");
        assert!(!is_simple_vote(1, false, 1, vote), "a versioned message is not a simple vote");
        assert!(!is_simple_vote(1, true, 2, vote), "a second instruction disqualifies");
        assert!(!is_simple_vote(1, true, 0, vote), "no instruction, nothing to vote with");
        assert!(!is_simple_vote(1, true, 1, Some(&other[..])), "another program");
        assert!(!is_simple_vote(1, true, 1, None), "no key to read means no claim");
    }

    #[test]
    fn each_grpc_mode_maps_to_its_own_kind_and_label() {
        use crate::config::GrpcMode;
        assert_eq!(SourceKind::from(GrpcMode::Transactions), SourceKind::Grpc);
        assert_eq!(
            SourceKind::from(GrpcMode::Deshred),
            SourceKind::GrpcDeshred
        );
        assert_eq!(SourceKind::Shred.label(), "shreds");
        assert_eq!(SourceKind::Grpc.label(), "grpc");
        assert_eq!(SourceKind::GrpcDeshred.label(), "grpc-deshred");
        assert_ne!(
            SourceKind::from(GrpcMode::Transactions).label(),
            SourceKind::from(GrpcMode::Deshred).label()
        );
    }

    #[test]
    fn first_seen_wins_and_dedupes() {
        let mut r = reg();
        r.record_first(0, sig(1), 1_000, 42, TxnMeta::default());
        // a later re-delivery of the same signature on the same source is ignored
        r.record_first(0, sig(1), 5_000, 42, TxnMeta::default());
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
        r.record_first(0, sig(1), 1_000, 42, TxnMeta::default());
        r.record_first(1, sig(1), 3_000, 42, TxnMeta::default());
        r.finalize(true);
        let raw = r.export();
        assert_eq!(r.contested_signatures(), 1);
        assert_eq!(raw[0].wins, 1, "shred delivered first");
        assert_eq!(raw[1].wins, 0);
        // grpc is 2us behind the earliest (shred), shred is 0 behind
        assert_eq!(raw[0].p50_us, Some(0.0));
        assert_eq!(raw[1].p50_us, Some(2.0));
    }

    #[test]
    fn grpc_ahead_wins() {
        let mut r = reg();
        r.record_first(1, sig(2), 2_000, 42, TxnMeta::default());
        r.record_first(0, sig(2), 9_000, 42, TxnMeta::default());
        r.finalize(true);
        let raw = r.export();
        assert_eq!(raw[1].wins, 1);
        assert_eq!(raw[0].wins, 0);
        assert_eq!(raw[0].p50_us, Some(7.0));
    }

    #[test]
    fn txn_rows_keep_lone_deliveries_duplicates_and_metadata() {
        let mut r = reg();
        r.enable_txn_rows();
        let meta = TxnMeta {
            server_created_at_ns: Some(900),
            is_vote: Some(true),
            message_size: Some(215),
            connection_id: Some(3),
        };
        r.record_first(1, sig(1), 1_000, 42, meta);
        r.record_first(1, sig(1), 9_000, 42, TxnMeta::default());
        r.record_first(0, sig(2), 2_000, 42, TxnMeta::default());
        r.finalize(true);

        let rows = r.drain_rows();
        assert_eq!(rows.len(), 2, "a lone delivery belongs in the table");
        let grpc = rows.iter().find(|x| x.sid == 1).unwrap();
        assert_eq!(grpc.first_rx_unix_ns, 1_000);
        assert_eq!(grpc.duplicate_count, 1);
        assert_eq!(grpc.meta, meta);
        assert_eq!(grpc.slot, 42);
        assert!(r.drain_rows().is_empty(), "rows are handed over, not copied");
    }

    #[test]
    fn txn_rows_are_not_collected_unless_asked() {
        let mut r = reg();
        r.record_first(0, sig(1), 1_000, 42, TxnMeta::default());
        r.finalize(true);
        assert!(r.drain_rows().is_empty());
    }

    #[test]
    fn slot_floor_eviction_keeps_events_bounded() {
        let mut r = reg();
        // an old contested signature at slot 10
        r.record_first(0, sig(1), 1_000, 10, TxnMeta::default());
        r.record_first(1, sig(1), 2_000, 10, TxnMeta::default());
        // tip advances well past the eviction margin
        r.record_first(0, sig(2), 3_000, 10 + EVICT_MARGIN_SLOTS + 5, TxnMeta::default());
        r.finalize(false);
        // slot-10 row is finalized and retired; the fresh row stays in flight
        assert_eq!(r.contested_signatures(), 1);
        assert_eq!(r.export()[0].wins, 1);
    }
}
