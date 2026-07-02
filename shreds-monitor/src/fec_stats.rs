use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use solana_ledger::shred::layout;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{leader_schedule::LeaderSchedule, metrics::Metrics};

const CODING_HEADER_LEN: usize = 89;
const VARIANT_OFFSET: usize = 64;
const VERSION_OFFSET: usize = 77;
const DATA_FLAGS_OFFSET: usize = 85;
const CODING_NUM_DATA_OFFSET: usize = 83;
const CODING_NUM_CODING_OFFSET: usize = 85;
const CODING_POSITION_OFFSET: usize = 87;
const KIND_MASK: u8 = 0xC0;
const KIND_CODE: u8 = 0x40;
const KIND_DATA: u8 = 0x80;
const LAST_IN_SLOT_FLAGS: u8 = 0b1100_0000;
const MAX_POSITION: u32 = 128;

const MAX_INFLIGHT_FLUSHES: usize = 4;
const SLOT_TIMINGS_TABLE: &str = "slot_timings";
const PROVIDERS_TABLE: &str = "providers";
const SLOT_TIMINGS_FLUSH: Duration = Duration::from_secs(15);
const SLOT_TIMINGS_KEEP_SLOTS: u64 = 5000;
const MAX_PLAUSIBLE_SLOT: u64 = 1 << 40;

pub struct FecShred {
    pub provider_id: u32,
    pub rx: Instant,
    pub shred: Bytes,
}

pub struct FecStatsParams {
    pub client: clickhouse::Client,
    pub table: String,
    pub baseline_provider_id: u32,
    pub shred_version: Option<u16>,
    pub fec_grace_ms: u64,
    pub fec_max_wait_slots: u64,
    pub flush_interval_ms: u64,
    pub batch_rows: usize,
    pub metrics: Arc<Metrics>,
}

#[derive(Default)]
struct ProviderFec {
    seen: std::collections::HashSet<u64>,
    data_pos: u128,
    coding_pos: u128,
    invalid: u32,
    duplicate: u32,
    first: Option<Instant>,
    last: Option<Instant>,
    decode: Option<Instant>,
}

impl ProviderFec {
    fn delivered(&self) -> u32 {
        (self.data_pos.count_ones() + self.coding_pos.count_ones()) as u32
    }
}

struct FecSet {
    root: Option<[u8; 32]>,
    sig_ok: Option<bool>,
    num_data: Option<u16>,
    num_coding: Option<u16>,
    last_in_slot: bool,
    earliest_valid: Option<Instant>,
    first_seen: Instant,
    providers: HashMap<u32, ProviderFec>,
}

impl FecSet {
    fn new(now: Instant) -> Self {
        Self {
            root: None,
            sig_ok: None,
            num_data: None,
            num_coding: None,
            last_in_slot: false,
            earliest_valid: None,
            first_seen: now,
            providers: HashMap::new(),
        }
    }

    fn evaluate(&mut self, s: &[u8], leader: Option<&Pubkey>, expect_version: Option<u16>) -> bool {
        if let Some(want) = expect_version {
            let v = u16::from_le_bytes([s[VERSION_OFFSET], s[VERSION_OFFSET + 1]]);
            if v != want {
                return false;
            }
        }
        let Some(root) = layout::get_merkle_root(s) else {
            return false;
        };
        let root = root.to_bytes();
        let valid = match self.root {
            None => {
                self.root = Some(root);
                true
            }
            Some(existing) => existing == root,
        };
        if valid && self.sig_ok.is_none() {
            if let (Some(leader), Ok(sig_bytes)) = (leader, <[u8; 64]>::try_from(&s[..64])) {
                let ok = Signature::from(sig_bytes).verify(leader.as_ref(), &root);
                self.sig_ok = Some(ok);
            }
        }
        valid
    }
}

#[derive(clickhouse::Row, serde::Serialize)]
struct Row {
    provider_id: u32,
    slot: u64,
    fec_set_index: u32,
    #[serde(rename = "fec_first_shred_delay_ns")]
    first_ns: Option<i64>,
    #[serde(rename = "fec_decode_delay_ns")]
    decode_ns: Option<i64>,
    #[serde(rename = "fec_last_shred_delay_ns")]
    last_ns: Option<i64>,
    #[serde(rename = "invalid_shreds")]
    invalid: u32,
    #[serde(rename = "missed_shreds")]
    missed: u32,
    #[serde(rename = "duplicated_shreds")]
    duplicated: u32,
    is_valid: bool,
}

#[derive(clickhouse::Row, serde::Serialize)]
struct SlotTimingRow {
    slot: u64,
    seen_at: i64,
}

#[derive(clickhouse::Row, serde::Serialize)]
struct ProviderRow {
    id: u32,
    name: String,
    updated_at: u32,
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn unix_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

/// Insert every row into `table` as a single ClickHouse batch.
async fn write_rows<R>(
    client: &clickhouse::Client,
    table: &str,
    rows: &[R],
) -> clickhouse::error::Result<()>
where
    R: clickhouse::RowOwned + clickhouse::RowWrite,
{
    let mut insert = client.insert::<R>(table).await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await
}

/// Like [`write_rows`], but for background flushes: logs failures instead of
/// returning them.
async fn write_rows_logged<R>(
    client: &clickhouse::Client,
    table: &str,
    rows: Vec<R>,
    metrics: &Metrics,
) where
    R: clickhouse::RowOwned + clickhouse::RowWrite,
{
    match write_rows(client, table, &rows).await {
        Ok(()) => metrics.record_rows_written(table, rows.len() as u64),
        Err(e) => {
            metrics.record_write_error(table);
            warn!(error = %e, rows = rows.len(), table, "fec_stats: clickhouse write failed");
        }
    }
}

pub async fn upsert_providers(
    client: &clickhouse::Client,
    providers: &[(u32, String)],
) -> anyhow::Result<()> {
    let now = unix_secs();
    let rows: Vec<ProviderRow> = providers
        .iter()
        .map(|(id, name)| ProviderRow {
            id: *id,
            name: name.clone(),
            updated_at: now,
        })
        .collect();
    write_rows(client, PROVIDERS_TABLE, &rows).await?;
    Ok(())
}

pub async fn run(
    params: FecStatsParams,
    mut rx: mpsc::Receiver<FecShred>,
    current_slot: Arc<AtomicU64>,
    schedule: Arc<LeaderSchedule>,
    cancel: CancellationToken,
) {
    info!(table = %params.table, "fec_stats: monitor starting");
    let flush_interval = Duration::from_millis(params.flush_interval_ms.max(1));
    let mut monitor = Monitor::new(params, schedule, current_slot);

    let mut sweep_tick = tokio::time::interval(flush_interval);
    let mut slot_tick = tokio::time::interval(SLOT_TIMINGS_FLUSH);
    sweep_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    slot_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            item = rx.recv() => {
                match item {
                    Some(item) => monitor.process(item),
                    None => break,
                }
                monitor.flush_if_full().await;
            }
            _ = sweep_tick.tick() => {
                monitor.sweep(false);
                monitor.flush().await;
            }
            _ = slot_tick.tick() => {
                monitor.flush_slot_timings().await;
            }
        }
    }

    // Drain what's still queued, then flush synchronously so the writes land
    // before this task returns.
    while let Ok(item) = rx.try_recv() {
        monitor.process(item);
    }
    monitor.flush_slot_timings().await;
    monitor.sweep(true);
    monitor.flush_blocking().await;
    info!("fec_stats: monitor exited");
}

/// Accumulates per-(slot, fec_set) shred observations and flushes finalized
/// stats to ClickHouse in batches.
struct Monitor {
    params: FecStatsParams,
    schedule: Arc<LeaderSchedule>,
    current_slot: Arc<AtomicU64>,
    inflight: Arc<Semaphore>,
    batch_rows: usize,
    sets: HashMap<(u64, u32), FecSet>,
    slot_seen: HashMap<u64, i64>,
    current_max_slot: u64,
    out: Vec<Row>,
}

impl Monitor {
    fn new(
        params: FecStatsParams,
        schedule: Arc<LeaderSchedule>,
        current_slot: Arc<AtomicU64>,
    ) -> Self {
        let batch_rows = params.batch_rows.max(1);
        Self {
            params,
            schedule,
            current_slot,
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_FLUSHES)),
            batch_rows,
            sets: HashMap::new(),
            slot_seen: HashMap::new(),
            current_max_slot: 0,
            out: Vec::with_capacity(batch_rows),
        }
    }

    /// Fold one received shred into its FEC set, updating per-provider timing
    /// and delivery bookkeeping.
    fn process(&mut self, item: FecShred) {
        let s = item.shred.as_ref();
        if s.len() < CODING_HEADER_LEN + 1 {
            return;
        }
        let Some(slot) = layout::get_slot(s) else {
            return;
        };
        if slot >= MAX_PLAUSIBLE_SLOT {
            return;
        }
        let Some(fec) = layout::get_fec_set_index(s) else {
            return;
        };
        let Some(index) = layout::get_index(s) else {
            return;
        };
        let kind = s[VARIANT_OFFSET] & KIND_MASK;
        if kind != KIND_CODE && kind != KIND_DATA {
            return;
        }

        let leader = self.schedule.leader(slot);
        let set = self
            .sets
            .entry((slot, fec))
            .or_insert_with(|| FecSet::new(item.rx));
        let structural = set.evaluate(s, leader.as_ref(), self.params.shred_version);

        if kind == KIND_CODE && set.num_data.is_none() {
            set.num_data = Some(u16::from_le_bytes([
                s[CODING_NUM_DATA_OFFSET],
                s[CODING_NUM_DATA_OFFSET + 1],
            ]));
            set.num_coding = Some(u16::from_le_bytes([
                s[CODING_NUM_CODING_OFFSET],
                s[CODING_NUM_CODING_OFFSET + 1],
            ]));
        }
        if kind == KIND_DATA && (s[DATA_FLAGS_OFFSET] & LAST_IN_SLOT_FLAGS) == LAST_IN_SLOT_FLAGS {
            set.last_in_slot = true;
        }

        let position = if kind == KIND_DATA {
            index.saturating_sub(fec)
        } else {
            u16::from_le_bytes([s[CODING_POSITION_OFFSET], s[CODING_POSITION_OFFSET + 1]]) as u32
        };

        let num_data = set.num_data;
        let rx = item.rx;
        let h = fast_hash(s);
        let e = set.providers.entry(item.provider_id).or_default();

        if !e.seen.insert(h) {
            e.duplicate += 1;
            if let Some(p) = self.params.metrics.provider(item.provider_id) {
                p.duplicate.inc();
            }
            return;
        }
        if !structural {
            e.invalid += 1;
            if let Some(p) = self.params.metrics.provider(item.provider_id) {
                p.invalid.inc();
            }
            return;
        }

        if slot > self.current_max_slot {
            self.current_max_slot = slot;
            self.current_slot.store(slot, Ordering::Relaxed);
        }
        self.slot_seen.entry(slot).or_insert_with(now_millis);

        if position < MAX_POSITION {
            if kind == KIND_DATA {
                e.data_pos |= 1u128 << position;
            } else {
                e.coding_pos |= 1u128 << position;
            }
        }
        e.first = Some(min_instant(e.first, rx));
        e.last = Some(max_instant(e.last, rx));
        if e.decode.is_none() {
            if let Some(nd) = num_data {
                if e.delivered() >= nd as u32 {
                    e.decode = Some(rx);
                }
            }
        }
        set.earliest_valid = Some(min_instant(set.earliest_valid, rx));
    }

    /// Move FEC sets that are complete, aged out, or timed out (or every set,
    /// when `drain_all`) into the pending output batch.
    fn sweep(&mut self, drain_all: bool) {
        let grace = Duration::from_millis(self.params.fec_grace_ms);
        let hard = grace.saturating_mul(4);
        let mut ready: Vec<(u64, u32)> = Vec::new();
        for (key, set) in self.sets.iter() {
            if drain_all {
                ready.push(*key);
                continue;
            }
            let aged = self.current_max_slot.saturating_sub(key.0) >= self.params.fec_max_wait_slots;
            let complete = set.last_in_slot && set.first_seen.elapsed() >= grace;
            let timed_out = set.first_seen.elapsed() >= hard;
            if aged || complete || timed_out {
                ready.push(*key);
            }
        }
        let baseline_id = self.params.baseline_provider_id;
        for key in ready {
            if let Some(set) = self.sets.remove(&key) {
                finalize_set(key, set, baseline_id, &mut self.out, &self.params.metrics);
            }
        }
    }

    /// Flush the pending batch once it reaches the configured size.
    async fn flush_if_full(&mut self) {
        if self.out.len() >= self.batch_rows {
            self.flush().await;
        }
    }

    /// Flush the pending batch synchronously, used on shutdown so the write
    /// completes before the task exits.
    async fn flush_blocking(&mut self) {
        if self.out.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.out);
        write_rows_logged(&self.params.client, &self.params.table, rows, &self.params.metrics)
            .await;
    }
}

fn finalize_set(
    key: (u64, u32),
    set: FecSet,
    baseline_id: u32,
    out: &mut Vec<Row>,
    metrics: &Metrics,
) {
    let (slot, fec) = key;
    let expected_total = match (set.num_data, set.num_coding) {
        (Some(d), Some(c)) => Some(d as u32 + c as u32),
        _ => None,
    };
    let baseline = if baseline_id != 0 {
        set.providers.get(&baseline_id)
    } else {
        None
    };
    let base_first = baseline.and_then(|b| b.first);
    let anchor_first = match base_first.or(set.earliest_valid) {
        Some(t) => t,
        None => return,
    };
    let anchor_decode = baseline
        .and_then(|b| b.decode)
        .or_else(|| earliest(set.providers.values().filter_map(|p| p.decode)));
    let anchor_last = baseline
        .and_then(|b| b.last)
        .or_else(|| earliest(set.providers.values().filter_map(|p| p.last)));

    let winner = set
        .providers
        .iter()
        .filter_map(|(id, e)| e.first.map(|t| (*id, t)))
        .min_by_key(|(_, t)| *t)
        .map(|(id, _)| id);
    if let Some(p) = winner.and_then(|id| metrics.provider(id)) {
        p.fec_sets_won.inc();
    }

    for (provider_id, e) in set.providers.iter() {
        if let (Some(first), Some(p)) = (e.first, metrics.provider(*provider_id)) {
            p.first_shred_delay_ns
                .observe(signed_delta(first, anchor_first) as f64);
        }
        let delivered = e.delivered();
        let exp = expected_total.unwrap_or(delivered);
        let missed = exp.saturating_sub(delivered);
        let is_valid = e.invalid == 0 && set.sig_ok == Some(true) && e.decode.is_some();
        out.push(Row {
            provider_id: *provider_id,
            slot,
            fec_set_index: fec,
            first_ns: e.first.map(|t| signed_delta(t, anchor_first)),
            decode_ns: e.decode.zip(anchor_decode).map(|(t, a)| signed_delta(t, a)),
            last_ns: e.last.zip(anchor_last).map(|(t, a)| signed_delta(t, a)),
            invalid: e.invalid,
            missed,
            duplicated: e.duplicate,
            is_valid,
        });
    }
}

impl Monitor {
    /// Spawn a background flush of the pending stats batch, bounded by the
    /// in-flight semaphore.
    async fn flush(&mut self) {
        if self.out.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.out);
        let permit = match self.inflight.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let client = self.params.client.clone();
        let table = self.params.table.clone();
        let metrics = Arc::clone(&self.params.metrics);
        tokio::spawn(async move {
            let _permit = permit;
            write_rows_logged(&client, &table, rows, &metrics).await;
        });
    }

    /// Prune the slot -> first-seen map, then spawn a background write of what
    /// remains.
    async fn flush_slot_timings(&mut self) {
        self.slot_seen.retain(|slot, _| *slot < MAX_PLAUSIBLE_SLOT);
        if self.current_max_slot > SLOT_TIMINGS_KEEP_SLOTS {
            let cutoff = self.current_max_slot - SLOT_TIMINGS_KEEP_SLOTS;
            self.slot_seen.retain(|slot, _| *slot >= cutoff);
        }
        if self.slot_seen.is_empty() {
            return;
        }
        let rows: Vec<SlotTimingRow> = self
            .slot_seen
            .iter()
            .map(|(slot, seen_at)| SlotTimingRow {
                slot: *slot,
                seen_at: *seen_at,
            })
            .collect();
        let permit = match self.inflight.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let client = self.params.client.clone();
        let metrics = Arc::clone(&self.params.metrics);
        tokio::spawn(async move {
            let _permit = permit;
            write_rows_logged(&client, SLOT_TIMINGS_TABLE, rows, &metrics).await;
        });
    }
}

#[inline]
fn fast_hash(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in s {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[inline]
fn earliest(iter: impl Iterator<Item = Instant>) -> Option<Instant> {
    iter.reduce(|a, b| if a <= b { a } else { b })
}

#[inline]
fn min_instant(cur: Option<Instant>, t: Instant) -> Instant {
    match cur {
        Some(c) if c <= t => c,
        _ => t,
    }
}

#[inline]
fn max_instant(cur: Option<Instant>, t: Instant) -> Instant {
    match cur {
        Some(c) if c >= t => c,
        _ => t,
    }
}

#[inline]
fn signed_delta(t: Instant, base: Instant) -> i64 {
    if t >= base {
        t.saturating_duration_since(base).as_nanos() as i64
    } else {
        -(base.saturating_duration_since(t).as_nanos() as i64)
    }
}
