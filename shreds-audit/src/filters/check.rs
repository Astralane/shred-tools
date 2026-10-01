use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use ahash::AHashMap;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{
    audit::{CheckTotals, FilterAudit, SlotTask, Window},
    db::{DbHandle, FetchRow, Row, SlotCheckRow, WindowRow},
    spec::{Key, TxView},
};
use crate::rpc::{RpcEndpoint, RpcError};
use crate::sigreg::SigRegistry;

const TICK: Duration = Duration::from_millis(500);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: u32 = 5;
const RETRY_AFTER: Duration = Duration::from_secs(4);
const BLOCK_CACHE: usize = 12;
const GIVE_UP_SECS: u64 = 600;
const LOCAL_VIOLATION_CAP: u64 = 25;
/// Hard ceiling on getBlock calls, whatever the windows ask for: at most one
/// every 500 ms. The steady state is one per `sample_every_slots` slots (~0.25/s
/// at 10), so this only ever bites when something is wrong — and then it keeps a
/// bug from turning into a flood against a paid provider.
const MIN_CALL_GAP: Duration = Duration::from_millis(500);
/// Network calls per tick; the rest wait for the next tick.
const MAX_CALLS_PER_TICK: usize = 4;
/// After an RPC error (429, transport, anything but "not available yet") every
/// fetch pauses, doubling from the first to the last of these until a success.
const BACKOFF_MIN: Duration = Duration::from_secs(5);
const BACKOFF_MAX: Duration = Duration::from_secs(120);

const RPC_SLOT_SKIPPED: i64 = -32007;
const RPC_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;
const RPC_BLOCK_NOT_AVAILABLE: i64 = -32004;

pub fn spawn(
    audit: Arc<FilterAudit>,
    reg: Arc<Mutex<SigRegistry>>,
    rpc: RpcEndpoint,
    lag_slots: u64,
    db: DbHandle,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>> {
    let worker = Worker {
        audit,
        reg,
        rpc,
        lag_slots,
        db,
        retries: VecDeque::new(),
        cache: VecDeque::new(),
        failing: false,
        last_call: None,
        backoff: None,
    };
    std::thread::Builder::new()
        .name("filter-check".into())
        .spawn(move || worker.run(cancel))
        .context("spawning the filter-audit checker")
}

struct Block {
    pub txs: Vec<TxView>,
    pub index: AHashMap<[u8; 64], usize>,
}

enum Fetched {
    Block(Arc<Block>),
    Skipped,
}

struct Retry {
    task: SlotTask,
    not_before: Instant,
}

struct Worker {
    audit: Arc<FilterAudit>,
    reg: Arc<Mutex<SigRegistry>>,
    rpc: RpcEndpoint,
    lag_slots: u64,
    db: DbHandle,
    retries: VecDeque<Retry>,
    cache: VecDeque<(u64, Arc<Block>)>,
    failing: bool,
    last_call: Option<Instant>,
    /// Paused until, and the pause to use next time.
    backoff: Option<(Instant, Duration)>,
}

impl Worker {
    fn run(mut self, cancel: CancellationToken) {
        while !cancel.is_cancelled() {
            self.tick();
            let until = Instant::now() + TICK;
            while Instant::now() < until && !cancel.is_cancelled() {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        for r in std::mem::take(&mut self.retries) {
            self.unchecked(&r.task);
        }
        for v in self.audit.drain_violations() {
            self.db.send(Row::Violation(v));
        }
        self.reg.lock().unwrap().finalize(true);
        for w in self.audit.take_all() {
            self.emit_window(w);
        }
    }

    fn tick(&mut self) {
        let mut tasks = self.audit.take_ready(self.lag_slots);
        let now = Instant::now();
        let (due, wait): (Vec<Retry>, Vec<Retry>) = std::mem::take(&mut self.retries)
            .into_iter()
            .partition(|r| r.not_before <= now);
        self.retries = wait.into();
        tasks.extend(due.into_iter().map(|r| r.task));

        let mut by_slot: BTreeMap<u64, Vec<SlotTask>> = BTreeMap::new();
        for t in tasks {
            by_slot.entry(t.slot).or_default().push(t);
        }
        let mut calls = 0;
        for (slot, tasks) in by_slot {
            let cached = self.cache.iter().any(|(s, _)| *s == slot);
            if !cached {
                let paused = self.backoff.and_then(|(until, _)| (until > Instant::now()).then_some(until));
                if paused.is_some() || calls >= MAX_CALLS_PER_TICK {
                    // Wait without spending an attempt: the slot is not at fault.
                    let not_before = paused.unwrap_or_else(Instant::now);
                    for task in tasks {
                        self.retries.push_back(Retry { task, not_before });
                    }
                    continue;
                }
                calls += 1;
            }
            let attempt = tasks.iter().map(|t| t.attempts).max().unwrap_or(0) + 1;
            match self.fetch(slot, attempt) {
                Some(fetched) => {
                    for t in tasks {
                        self.compare(t, &fetched);
                    }
                }
                None if attempt < MAX_ATTEMPTS => {
                    for mut task in tasks {
                        task.attempts = attempt;
                        self.retries.push_back(Retry {
                            task,
                            not_before: Instant::now() + RETRY_AFTER,
                        });
                    }
                }
                None => {
                    for t in &tasks {
                        self.unchecked(t);
                    }
                }
            }
        }

        for v in self.audit.drain_violations() {
            self.db.send(Row::Violation(v));
        }
        for w in self.audit.take_finished(GIVE_UP_SECS) {
            self.emit_window(w);
        }
    }

    fn unchecked(&self, t: &SlotTask) {
        self.audit.with_window(t.key, |w| {
            w.in_flight = w.in_flight.saturating_sub(1);
            w.slots_unchecked += 1;
        });
    }

    fn fetch(&mut self, slot: u64, attempt: u32) -> Option<Fetched> {
        if let Some((_, b)) = self.cache.iter().find(|(s, _)| *s == slot) {
            return Some(Fetched::Block(b.clone()));
        }
        if let Some(last) = self.last_call {
            let wait = MIN_CALL_GAP.saturating_sub(last.elapsed());
            if !wait.is_zero() {
                std::thread::sleep(wait);
            }
        }
        self.last_call = Some(Instant::now());
        let started = Instant::now();
        let result = get_block(&self.rpc, slot);
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut row = FetchRow {
            ts: SystemTime::now(),
            rpc: self.rpc.label(),
            slot,
            attempt,
            status: "ok",
            latency_ms,
            block_txs: None,
            error: None,
        };
        let out = match result {
            Ok(Some(block)) => {
                row.block_txs = Some(block.txs.len() as u32);
                let block = Arc::new(block);
                self.cache.push_back((slot, block.clone()));
                if self.cache.len() > BLOCK_CACHE {
                    self.cache.pop_front();
                }
                Some(Fetched::Block(block))
            }
            Ok(None) => {
                row.status = "skipped";
                Some(Fetched::Skipped)
            }
            Err(e) => {
                let not_available = e
                    .downcast_ref::<RpcError>()
                    .is_some_and(|r| r.code == RPC_BLOCK_NOT_AVAILABLE);
                row.status = if not_available {
                    "not_available"
                } else {
                    "error"
                };
                row.error = Some(format!("{e:#}"));
                if !not_available {
                    let pause = self.backoff.map_or(BACKOFF_MIN, |(_, p)| p);
                    self.backoff = Some((Instant::now() + pause, (pause * 2).min(BACKOFF_MAX)));
                    if !self.failing {
                        self.failing = true;
                        eprintln!(
                            "filter-audit: getBlock({slot}) via {} failed: {e:#} — backing off",
                            self.rpc.label()
                        );
                    }
                }
                None
            }
        };
        if out.is_some() {
            self.backoff = None;
            if self.failing {
                self.failing = false;
                eprintln!("filter-audit: getBlock recovered");
            }
        }
        self.db.send(Row::Fetch(row));
        out
    }

    fn compare(&self, task: SlotTask, fetched: &Fetched) {
        let Some((source, kind, started_at)) = self.audit.with_window(task.key, |w| {
            (w.info.source.clone(), w.info.kind.label(), w.started_at)
        }) else {
            return;
        };
        let bundle = &task.bundle;
        let (block_status, block_txs) = match fetched {
            Fetched::Block(b) => ("ok", b.txs.len() as u32),
            Fetched::Skipped => ("skipped", 0),
        };

        let mut totals = Vec::with_capacity(bundle.filters.len());
        let mut examples: Vec<(usize, &'static str, [u8; 64], String)> = Vec::new();
        for (i, f) in bundle.filters.iter().enumerate() {
            let bit = 1u32 << i;
            let delivered: Vec<&[u8; 64]> = task
                .deliveries
                .iter()
                .filter(|(_, &m)| m & bit != 0)
                .map(|(s, _)| s)
                .collect();
            let mut t = CheckTotals {
                delivered: delivered.len() as u64,
                ..Default::default()
            };
            match fetched {
                Fetched::Skipped => {
                    t.extra_not_in_block = t.delivered;
                    for sig in delivered.iter().take(LOCAL_VIOLATION_CAP as usize) {
                        examples.push((
                            i,
                            "extra_not_in_block",
                            **sig,
                            "the cluster produced no block for this slot".into(),
                        ));
                    }
                }
                Fetched::Block(b) => {
                    for tx in b.txs.iter().filter(|tx| f.matches(tx)) {
                        t.expected += 1;
                        let mask = task.deliveries.get(&tx.signature);
                        if mask.is_some_and(|m| m & bit != 0) {
                            t.matched += 1;
                            continue;
                        }
                        t.missed += 1;
                        if t.missed <= LOCAL_VIOLATION_CAP {
                            let how = if mask.is_some() {
                                "delivered under other tags only"
                            } else {
                                "not delivered"
                            };
                            let why = format!("{how}; filter matches: {}", f.match_reason(tx));
                            examples.push((i, "missed", tx.signature, why));
                        }
                    }
                    for sig in &delivered {
                        match b.index.get(*sig) {
                            None => {
                                t.extra_not_in_block += 1;
                                if t.extra_not_in_block <= LOCAL_VIOLATION_CAP {
                                    examples.push((
                                        i,
                                        "extra_not_in_block",
                                        **sig,
                                        "not in the confirmed block".into(),
                                    ));
                                }
                            }
                            Some(&idx) => {
                                let Some(why) = f.mismatch(&b.txs[idx]) else {
                                    continue;
                                };
                                t.extra_in_block += 1;
                                if t.extra_in_block <= LOCAL_VIOLATION_CAP {
                                    examples.push((
                                        i,
                                        "extra_in_block",
                                        **sig,
                                        format!("in the block, filter rejects it: {why}"),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            self.db.send(Row::Slot(SlotCheckRow {
                ts: SystemTime::now(),
                source: source.clone(),
                kind,
                window_started_at: started_at,
                bundle: bundle.name.clone(),
                filter: f.name.clone(),
                slot: task.slot,
                block_status,
                block_txs,
                expected: t.expected as u32,
                delivered: t.delivered as u32,
                matched: t.matched as u32,
                missed: t.missed as u32,
                extra_in_block: t.extra_in_block as u32,
                extra_not_in_block: t.extra_not_in_block as u32,
            }));
            totals.push(t);
        }

        self.audit.with_window(task.key, |w| {
            w.in_flight = w.in_flight.saturating_sub(1);
            match fetched {
                Fetched::Block(_) => w.slots_checked += 1,
                Fetched::Skipped => w.slots_skipped += 1,
            }
            for (i, t) in totals.iter().enumerate() {
                w.totals[i].add(t);
            }
            for (i, kind, sig, why) in examples {
                w.violation(Some(i), kind, task.slot, &sig, why);
            }
        });
    }

    fn emit_window(&self, mut w: Window) {
        let lat = self
            .reg
            .lock()
            .unwrap()
            .take_window_latency(w.key.0, w.key.1);
        let (contested, wins, behind, vs_n, vs) = match lat {
            Some(mut l) => {
                let behind = [0.5, 0.9, 0.99].map(|q| {
                    (!l.behind_us.is_empty()).then(|| l.behind_us.value_at_quantile(q) as f64)
                });
                let vs = l.vs_shred_us.percentiles(&[0.1, 0.5, 0.9, 0.99]);
                (
                    l.contested,
                    l.wins,
                    behind,
                    l.vs_shred_us.count(),
                    [vs[0], vs[1], vs[2], vs[3]],
                )
            }
            None => (0, 0, [None; 3], 0, [None; 4]),
        };
        let sd = w.server_delay_us.percentiles(&[0.5, 0.9, 0.99]);
        let mut sum = CheckTotals::default();
        for t in &w.totals {
            sum.add(t);
        }
        let bundle = &w.info.bundle;
        let tagged: serde_json::Map<String, serde_json::Value> = bundle
            .filters
            .iter()
            .zip(&w.tagged)
            .map(|(f, n)| (f.name.clone(), (*n).into()))
            .collect();
        let row = WindowRow {
            source: w.info.source.clone(),
            kind: w.info.kind.label(),
            commitment: w.info.commitment.clone(),
            connection_id: w.key.1,
            started_at: w.started_at,
            ended_at: w.ended_at.unwrap_or_else(SystemTime::now),
            duration_secs: w.duration_secs(),
            bundle: bundle.name.clone(),
            filters_json: bundle.json.clone(),
            end_reason: w.end_reason.clone().unwrap_or_else(|| "shutdown".into()),
            connect_ms: w.connect_ms,
            first_msg_ms: w.first_msg_ms,
            tip_open: w.tip_open,
            tip_close: w.tip_close,
            audit_start_slot: w.audit_start,
            audit_end_slot: w.audit_end,
            delivered: w.delivered,
            tagged_json: serde_json::Value::Object(tagged).to_string(),
            untagged_updates: w.untagged_updates,
            unknown_tags: w.unknown_tags,
            duplicates: w.duplicates,
            tag_false_positive: w.tag_false_positive.iter().sum(),
            tag_missing: w.tag_missing.iter().sum(),
            vote_flag_mismatch: w.vote_flag_mismatch,
            slots_checked: w.slots_checked,
            slots_skipped: w.slots_skipped,
            slots_unchecked: w.slots_unchecked,
            expected: sum.expected,
            matched: sum.matched,
            missed: sum.missed,
            extra_in_block: sum.extra_in_block,
            extra_not_in_block: sum.extra_not_in_block,
            contested,
            wins,
            behind_us: behind,
            vs_shred_n: vs_n,
            vs_shred_us: vs,
            server_delay_us: [sd[0], sd[1], sd[2]],
        };
        if w.tip_open.is_some() || w.delivered > 0 {
            eprintln!(
                "filter-audit: {} [{}] {:.0}s ({}): delivered {} | {} slots: expected {} \
                 missed {} extra {}+{} | tags fp {} missing {} | vs shreds p50 {}",
                row.source,
                row.bundle,
                row.duration_secs,
                row.end_reason,
                row.delivered,
                row.slots_checked,
                row.expected,
                row.missed,
                row.extra_in_block,
                row.extra_not_in_block,
                row.tag_false_positive,
                row.tag_missing,
                row.vs_shred_us[1].map_or_else(|| "—".into(), |v| format!("{:.1}ms", v / 1000.0)),
            );
        }
        self.db.send(Row::Window(Box::new(row)));
    }
}

#[derive(Deserialize)]
struct JBlock {
    #[serde(default)]
    transactions: Vec<JTx>,
}

#[derive(Deserialize)]
struct JTx {
    transaction: JTransaction,
    meta: Option<JMeta>,
    #[serde(default)]
    version: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct JTransaction {
    signatures: Vec<String>,
    message: JMessage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JMessage {
    account_keys: Vec<String>,
    #[serde(default)]
    instructions: Vec<JInstruction>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JInstruction {
    program_id_index: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JMeta {
    err: Option<serde_json::Value>,
    #[serde(default)]
    loaded_addresses: Option<JLoaded>,
}

#[derive(Deserialize)]
struct JLoaded {
    #[serde(default)]
    writable: Vec<String>,
    #[serde(default)]
    readonly: Vec<String>,
}

fn key(s: &str) -> Result<Key> {
    let raw = bs58::decode(s).into_vec()?;
    raw.as_slice()
        .try_into()
        .map_err(|_| anyhow!("key `{s}` is {} bytes", raw.len()))
}

fn get_block(rpc: &RpcEndpoint, slot: u64) -> Result<Option<Block>> {
    let params = serde_json::json!([slot, {
        "encoding": "json",
        "transactionDetails": "full",
        "rewards": false,
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 1,
    }]);
    match rpc.call("getBlock", params, RPC_TIMEOUT) {
        Ok(block) => parse_block(block).map(Some),
        Err(e) => match e.downcast_ref::<RpcError>() {
            Some(r) if matches!(r.code, RPC_SLOT_SKIPPED | RPC_LONG_TERM_STORAGE_SLOT_SKIPPED) => {
                Ok(None)
            }
            _ => Err(e),
        },
    }
}

fn parse_block(block: JBlock) -> Result<Block> {
    let mut txs = Vec::with_capacity(block.transactions.len());
    let mut index = AHashMap::with_capacity(block.transactions.len());
    for jt in block.transactions {
        let first = jt
            .transaction
            .signatures
            .first()
            .ok_or_else(|| anyhow!("block transaction without a signature"))?;
        let raw = bs58::decode(first).into_vec()?;
        let signature: [u8; 64] = raw
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("signature of {} bytes", raw.len()))?;
        let mut keys = jt
            .transaction
            .message
            .account_keys
            .iter()
            .map(|k| key(k))
            .collect::<Result<Vec<_>>>()?;
        let num_static_keys = keys.len();
        let (failed, loaded) = match &jt.meta {
            Some(m) => (Some(m.err.is_some()), m.loaded_addresses.as_ref()),
            None => (None, None),
        };
        if let Some(l) = loaded {
            for k in l.writable.iter().chain(&l.readonly) {
                keys.push(key(k)?);
            }
        }
        let ixs = &jt.transaction.message.instructions;
        let first_program = ixs
            .first()
            .and_then(|ix| keys[..num_static_keys].get(ix.program_id_index).copied());
        let legacy = jt.version.as_ref().is_none_or(|v| v.as_str() == Some("legacy"));
        index.insert(signature, txs.len());
        txs.push(TxView {
            signature,
            num_signatures: jt.transaction.signatures.len(),
            legacy,
            keys,
            num_static_keys,
            num_instructions: ixs.len(),
            first_program,
            failed,
        });
    }
    Ok(Block { txs, index })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigreg::VOTE_PROGRAM_ID;

    fn b58(b: &[u8]) -> String {
        bs58::encode(b).into_string()
    }

    #[test]
    fn a_block_parses_into_filter_views() {
        let vote = b58(&VOTE_PROGRAM_ID);
        let json = serde_json::json!({
            "transactions": [
                {
                    "transaction": {
                        "signatures": [b58(&[1; 64])],
                        "message": {
                            "accountKeys": [b58(&[9; 32]), vote],
                            "instructions": [{"programIdIndex": 1, "accounts": [], "data": ""}]
                        }
                    },
                    "meta": {"err": null, "loadedAddresses": {"writable": [], "readonly": []}},
                    "version": "legacy"
                },
                {
                    "transaction": {
                        "signatures": [b58(&[2; 64])],
                        "message": {
                            "accountKeys": [b58(&[9; 32]), b58(&[7; 32])],
                            "instructions": [{"programIdIndex": 1}, {"programIdIndex": 1}]
                        }
                    },
                    "meta": {"err": {"InstructionError": [0, "Custom"]},
                             "loadedAddresses": {"writable": [b58(&[5; 32])], "readonly": [b58(&[6; 32])]}},
                    "version": 0
                }
            ]
        });
        let block = parse_block(serde_json::from_value(json).unwrap()).unwrap();
        assert_eq!(block.txs.len(), 2);
        let (v, u) = (&block.txs[0], &block.txs[1]);
        assert!(v.is_vote());
        assert_eq!(v.failed, Some(false));
        assert!(!u.is_vote());
        assert!(!u.legacy);
        assert_eq!(u.failed, Some(true));
        assert_eq!(u.num_static_keys, 2);
        assert_eq!(u.keys.len(), 4, "loaded addresses are account keys too");
        assert_eq!(block.index[&[2u8; 64]], 1);
    }
}

#[cfg(test)]
mod live {
    use super::*;
    use crate::filters::bundles;
    use crate::rpc::RpcCfg;

    #[test]
    #[ignore]
    fn live_block_parses_and_filters_split_it_sensibly() {
        let url = std::env::var("LIVE_RPC_URL").expect("LIVE_RPC_URL");
        let rpc = RpcCfg::generic(&url).resolve().unwrap();
        let tip: u64 = rpc
            .call(
                "getSlot",
                serde_json::json!([{"commitment": "confirmed"}]),
                RPC_TIMEOUT,
            )
            .unwrap();
        let (slot, block) = (0..20)
            .find_map(|back| {
                let s = tip - 40 - back;
                get_block(&rpc, s).unwrap().map(|b| (s, b))
            })
            .expect("a produced block in the last 20 slots");
        let votes = block.txs.iter().filter(|t| t.is_vote()).count();
        let with_alt = block
            .txs
            .iter()
            .filter(|t| t.keys.len() > t.num_static_keys)
            .count();
        let failed = block.txs.iter().filter(|t| t.failed == Some(true)).count();
        eprintln!(
            "slot {slot}: {} txs, {votes} votes, {with_alt} using lookup tables, {failed} failed",
            block.txs.len()
        );
        assert!(
            votes > 0 && votes < block.txs.len(),
            "a mainnet block has votes and user txs"
        );
        for cfg in bundles::defaults() {
            let b = bundles::Bundle::new(&cfg).unwrap();
            for f in &b.filters {
                let n = block.txs.iter().filter(|t| f.matches(t)).count();
                eprintln!("  {:<16} {:<16} {n}", b.name, f.name);
                if f.name == "never" {
                    assert_eq!(n, 0);
                }
                if f.name == "all" {
                    assert_eq!(n, block.txs.len());
                }
            }
        }
        let split = |name: &str| {
            let b = bundles::Bundle::new(
                &bundles::defaults()
                    .into_iter()
                    .find(|b| b.name == name)
                    .unwrap(),
            )
            .unwrap();
            b.filters
                .iter()
                .map(|f| block.txs.iter().filter(|t| f.matches(t)).count())
                .sum::<usize>()
        };
        assert_eq!(
            split("votes_split"),
            block.txs.len(),
            "vote and non-vote partition the block"
        );
    }
}
