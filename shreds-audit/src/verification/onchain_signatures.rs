//! Audit every transaction source against the block the cluster actually produced.
//!
//! The race in `sigreg` answers "who delivered this signature first". It is
//! deliberately blind to whether the signature is *real*: a source that invents
//! transactions, silently drops half a block, or repeats itself wins exactly the
//! same races as one that relays the leader's block faithfully. Deshredded feeds
//! are the sharp end of this — they report transactions *before* execution, so
//! nothing downstream in this tool ever confirms the transaction existed.
//!
//! This module closes that gap without touching the hot path. Every signature a
//! source reports is filed into a small per-slot index as it is recorded. On a
//! timer (`onchain_sample_secs`, default 5) a worker picks one settled slot, asks
//! an RPC node for that block's signature list with `getBlock`, and diffs the two:
//!
//!   * `missed`     — onchain, this source never delivered it
//!   * `corrupted`  — this source delivered it, the block does not contain it
//!   * `duplicated` — delivered more than once for the same slot
//!
//! Sampling rather than checking every slot is deliberate: the cluster produces
//! ~2.5 slots/s, so auditing all of them would mean a `getBlock` call per slot
//! forever, and the *rates* this estimates converge over a capture of any length
//! without it. It is a sampled audit, not a ledger — the counts are of sampled
//! slots, and the percentage is the number to read.
//!
//! A source is scored on a sampled slot only if it delivered *something* for it.
//! A slot it was completely absent for lands in `slots_absent` instead, so a feed
//! that was merely disconnected — or subscribed at a commitment that lags past
//! the sampling window — is never reported as a feed that corrupted a block.

use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::sigreg::SigRegistry;

/// A signature reduced to its leading 16 bytes.
///
/// The full 64-byte signature would cost 4x the memory for a window of slots
/// across every source, for nothing: an ed25519 signature's first 32 bytes are
/// the `R` curve point, which differs for every distinct transaction. Two
/// different transactions colliding on a 128-bit prefix does not happen by
/// accident, and producing one on purpose means finding a valid signature with a
/// chosen prefix — 2^128 work, not an attack a relay can mount.
type SigKey = u128;

fn sig_key(sig: &[u8; 64]) -> SigKey {
    let mut head = [0u8; 16];
    head.copy_from_slice(&sig[..16]);
    u128::from_le_bytes(head)
}

/// How far past the sampling lag the index keeps a slot. The worker claims a
/// slot's data *before* it makes the RPC call, so this margin only has to absorb
/// jitter in when sources report a slot, never the RPC latency.
const RETAIN_MARGIN_SLOTS: u64 = 16;

/// `getBlock` is a heavier call than the leader-schedule fetches and runs on a
/// timer, so it gets a shorter leash than `leader.rs` uses — a stalled endpoint
/// must not wedge the sampler for half a minute.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Granularity of the sampling sleep. Short enough that shutdown is prompt at any
/// configured interval.
const SLEEP_STEP: Duration = Duration::from_millis(100);

// JSON-RPC codes that mean "there is no block here", which is normal: leaders
// skip slots. These are not errors and must never be counted as one, or a
// healthy capture would report a rising RPC failure count.
const RPC_BLOCK_CLEANED_UP: i64 = -32001;
const RPC_BLOCK_NOT_AVAILABLE: i64 = -32004;
const RPC_SLOT_SKIPPED: i64 = -32007;
const RPC_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;

/// What one source delivered for one slot.
#[derive(Default)]
pub struct SourceSlot {
    sigs: AHashSet<SigKey>,
    /// Re-deliveries of a signature this source had already reported for this
    /// slot. A transaction appears in a block exactly once, so any repeat is a
    /// discrepancy with the block regardless of what else the source got right.
    duplicated: u64,
}

impl SourceSlot {
    fn is_empty(&self) -> bool {
        self.sigs.is_empty() && self.duplicated == 0
    }
}

struct SlotEntry {
    /// One entry per source id, indexed the same way as the signature registry.
    sources: Vec<SourceSlot>,
}

/// Per-slot, per-source signature sets over a short rolling window.
///
/// Lives inside [`SigRegistry`] so recording costs nothing beyond the lock the
/// caller already holds. Bounded by construction: only slots within
/// `retain_slots` of the highest slot seen are kept, so a run of any length holds
/// at most that many slots of signatures.
pub struct SlotSigIndex {
    n_sources: usize,
    slots: AHashMap<u64, SlotEntry>,
    tip: u64,
    retain_slots: u64,
    enabled: bool,
}

impl SlotSigIndex {
    /// An index that records nothing. The registry always holds one, so the
    /// recording call site stays unconditional; enabling it is the opt-in.
    pub fn disabled() -> Self {
        Self {
            n_sources: 0,
            slots: AHashMap::new(),
            tip: 0,
            retain_slots: 0,
            enabled: false,
        }
    }

    pub fn enable(&mut self, n_sources: usize, retain_slots: u64) {
        self.n_sources = n_sources;
        self.retain_slots = retain_slots.max(1);
        self.enabled = true;
    }

    /// Highest slot any source has reported. Same value the registry races on.
    pub fn tip(&self) -> u64 {
        self.tip
    }

    /// File one delivery. Called for every `record_first`, *before* its dedupe,
    /// so a repeat delivery is visible here even though the race ignores it.
    pub fn record(&mut self, sid: usize, slot: u64, sig: &[u8; 64]) {
        if !self.enabled || slot == 0 || sid >= self.n_sources {
            return;
        }
        if slot > self.tip {
            self.tip = slot;
            // The map holds at most `retain_slots` entries, so this sweep walks a
            // few dozen keys — cheap enough to run on every slot advance rather
            // than keeping a separate schedule to get it wrong.
            let floor = self.tip.saturating_sub(self.retain_slots);
            self.slots.retain(|&s, _| s >= floor);
        }
        if slot < self.tip.saturating_sub(self.retain_slots) {
            return; // too far behind to ever be sampled
        }

        let n = self.n_sources;
        let entry = self.slots.entry(slot).or_insert_with(|| SlotEntry {
            sources: (0..n).map(|_| SourceSlot::default()).collect(),
        });
        let s = &mut entry.sources[sid];
        if !s.sigs.insert(sig_key(sig)) {
            s.duplicated += 1;
        }
    }

    /// Remove and return everything recorded for `slot`.
    ///
    /// The worker claims the slot before it calls out to RPC, so the window can
    /// keep advancing during the call without the sample being pruned underneath
    /// it. A straggler arriving for a claimed slot re-creates an entry that is
    /// never sampled again and simply ages out.
    pub fn take(&mut self, slot: u64) -> Option<Vec<SourceSlot>> {
        self.slots.remove(&slot).map(|e| e.sources)
    }
}

/// One source's running discrepancy totals over the sampled slots.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct SourceAudit {
    /// Sampled slots this source delivered something for, and was scored on.
    pub slots_checked: u64,
    /// Sampled slots this source delivered nothing for. Not scored — see the
    /// module docs.
    pub slots_absent: u64,
    /// Transactions in the blocks this source was scored against. The
    /// denominator of the bad-signature rate.
    pub onchain_txns: u64,
    pub missed: u64,
    pub corrupted: u64,
    pub duplicated: u64,
}

impl SourceAudit {
    /// Any discrepancy with the block, however it arose.
    pub fn bad(&self) -> u64 {
        self.missed + self.corrupted + self.duplicated
    }

    /// Discrepancies as a fraction of the transactions actually in the sampled
    /// blocks. `None` until this source has been scored on a slot.
    pub fn bad_fraction(&self) -> Option<f64> {
        (self.onchain_txns > 0).then(|| self.bad() as f64 / self.onchain_txns as f64)
    }
}

/// Whole-audit state, shared between the sampling worker and the snapshot
/// builder. Kept behind its own lock so reading it never contends with the
/// registry's hot `record_first` path.
#[derive(Clone, Default)]
pub struct OnchainAudit {
    /// One entry per source id.
    pub sources: Vec<SourceAudit>,
    /// Slots successfully fetched and compared.
    pub slots_checked: u64,
    /// Slots the cluster produced no block for (skipped leader, or pruned).
    /// Normal, and never counted as an error.
    pub slots_unavailable: u64,
    /// `getBlock` calls that failed. A nonzero count means the audit is sampling
    /// less than it appears to, so it is surfaced rather than swallowed.
    pub rpc_errors: u64,
    /// Most recent RPC failure, for the manifest and the TUI footer.
    pub last_error: Option<String>,
}

impl OnchainAudit {
    fn new(n_sources: usize) -> Self {
        Self {
            sources: vec![SourceAudit::default(); n_sources],
            ..Default::default()
        }
    }
}

/// The sampling worker: one thread, one `getBlock` per second.
pub struct OnchainVerifier {
    audit: Arc<Mutex<OnchainAudit>>,
    handle: Option<JoinHandle<()>>,
}

impl OnchainVerifier {
    /// Enable the per-slot index on `reg` and start sampling.
    ///
    /// `lag_slots` is how far behind the tip a slot is sampled — far enough that
    /// every source has had time to deliver it and the block is available over
    /// RPC, close enough that the window stays small. `sample_secs` is the gap
    /// between sampled slots, one `getBlock` call each.
    pub fn start(
        rpc_url: String,
        lag_slots: u64,
        sample_secs: u64,
        n_sources: usize,
        reg: Arc<Mutex<SigRegistry>>,
        cancel: CancellationToken,
    ) -> Self {
        let retain = lag_slots + RETAIN_MARGIN_SLOTS;
        reg.lock().unwrap().enable_onchain_index(n_sources, retain);

        let audit = Arc::new(Mutex::new(OnchainAudit::new(n_sources)));
        let worker = Worker {
            rpc: RpcClient { url: rpc_url },
            lag_slots,
            // Guarded in config validation; floored here so a caller that skips
            // that path still cannot spin.
            tick: Duration::from_secs(sample_secs.max(1)),
            reg,
            audit: audit.clone(),
            last_sampled: 0,
            announced_error: false,
        };
        let handle = std::thread::Builder::new()
            .name("onchain-verify".into())
            .spawn(move || worker.run(cancel))
            .ok();

        Self { audit, handle }
    }

    /// Copy of the current totals. Cheap; holds the audit lock only for the copy.
    pub fn snapshot(&self) -> OnchainAudit {
        self.audit.lock().unwrap().clone()
    }

    /// Join the worker. The caller cancels the shared token first.
    pub fn finish(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

struct Worker {
    rpc: RpcClient,
    lag_slots: u64,
    /// Gap between sampled slots.
    tick: Duration,
    reg: Arc<Mutex<SigRegistry>>,
    audit: Arc<Mutex<OnchainAudit>>,
    last_sampled: u64,
    /// Latch so a dead endpoint is loud once instead of once a second.
    announced_error: bool,
}

impl Worker {
    fn run(mut self, cancel: CancellationToken) {
        while wait_for_tick(&cancel, self.tick) {
            self.sample();
        }
    }

    fn sample(&mut self) {
        // Claim the slot under the registry lock, then release it for the RPC
        // call. The lock guards the whole racing path; it must never be held
        // across the network.
        let (slot, sources) = {
            let mut reg = self.reg.lock().unwrap();
            let tip = reg.onchain_tip();
            let target = tip.saturating_sub(self.lag_slots);
            if target == 0 || target <= self.last_sampled {
                return;
            }
            match reg.take_onchain_slot(target) {
                // No source reported anything for this slot: nothing to audit,
                // and no evidence of a fault either. Advance past it.
                None => {
                    self.last_sampled = target;
                    return;
                }
                Some(sources) => (target, sources),
            }
        };
        self.last_sampled = slot;

        let onchain = match self.rpc.block_signatures(slot) {
            Ok(Some(sigs)) => sigs,
            Ok(None) => {
                self.audit.lock().unwrap().slots_unavailable += 1;
                return;
            }
            Err(e) => {
                let mut audit = self.audit.lock().unwrap();
                audit.rpc_errors += 1;
                audit.last_error = Some(format!("{e:#}"));
                drop(audit);
                if !self.announced_error {
                    self.announced_error = true;
                    eprintln!("onchain-verify: getBlock({slot}) failed: {e:#}");
                }
                return;
            }
        };

        let mut audit = self.audit.lock().unwrap();
        compare_slot(&onchain, &sources, &mut audit);
    }
}

/// Fold one sampled slot into the running totals.
///
/// Split out from the worker so the arithmetic is testable without a network.
fn compare_slot(onchain: &AHashSet<SigKey>, sources: &[SourceSlot], audit: &mut OnchainAudit) {
    audit.slots_checked += 1;
    for (sid, s) in sources.iter().enumerate() {
        let Some(a) = audit.sources.get_mut(sid) else {
            continue;
        };
        if s.is_empty() {
            a.slots_absent += 1;
            continue;
        }
        a.slots_checked += 1;
        a.onchain_txns += onchain.len() as u64;
        let hit = s.sigs.iter().filter(|k| onchain.contains(k)).count() as u64;
        a.missed += onchain.len() as u64 - hit;
        a.corrupted += s.sigs.len() as u64 - hit;
        a.duplicated += s.duplicated;
    }
}

/// Sleep until the next sample, in short steps so shutdown stays prompt however
/// long the interval is. Returns `false` once cancelled.
fn wait_for_tick(cancel: &CancellationToken, tick: Duration) -> bool {
    let deadline = Instant::now() + tick;
    loop {
        if cancel.is_cancelled() {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        std::thread::sleep(SLEEP_STEP.min(deadline - now));
    }
}

struct RpcClient {
    url: String,
}

#[derive(Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct BlockSignatures {
    #[serde(default)]
    signatures: Vec<String>,
}

impl RpcClient {
    /// The block's transaction signatures, or `None` when the cluster produced no
    /// block for that slot.
    ///
    /// `transactionDetails: "signatures"` is the cheapest form of `getBlock` that
    /// still answers the question — the node returns the first signature of every
    /// transaction in the block and nothing else, which is exactly the key every
    /// source is matched on.
    fn block_signatures(&self, slot: u64) -> Result<Option<AHashSet<SigKey>>> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [slot, {
                "encoding": "json",
                "transactionDetails": "signatures",
                "rewards": false,
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }],
        });
        let resp: RpcEnvelope<BlockSignatures> = ureq::post(&self.url)
            .set("content-type", "application/json")
            .timeout(RPC_TIMEOUT)
            .send_json(body)?
            .into_json()?;

        if let Some(err) = resp.error {
            if is_no_block(err.code) {
                return Ok(None);
            }
            return Err(anyhow!("rpc getBlock error {}: {}", err.code, err.message));
        }
        let block = resp
            .result
            .ok_or_else(|| anyhow!("rpc getBlock: empty result"))?;

        let mut out = AHashSet::with_capacity(block.signatures.len());
        for s in &block.signatures {
            // A signature we cannot decode is the node's problem, not a source's.
            // Skipping it would silently shrink the denominator and understate
            // every source's miss rate, so refuse the whole sample instead.
            let raw = bs58::decode(s)
                .into_vec()
                .map_err(|e| anyhow!("undecodable signature in block {slot}: {e}"))?;
            let sig: [u8; 64] = raw
                .as_slice()
                .try_into()
                .map_err(|_| anyhow!("signature of {} bytes in block {slot}", raw.len()))?;
            out.insert(sig_key(&sig));
        }
        Ok(Some(out))
    }
}

fn is_no_block(code: i64) -> bool {
    matches!(
        code,
        RPC_SLOT_SKIPPED
            | RPC_LONG_TERM_STORAGE_SLOT_SKIPPED
            | RPC_BLOCK_NOT_AVAILABLE
            | RPC_BLOCK_CLEANED_UP
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(n: u8) -> [u8; 64] {
        let mut s = [0u8; 64];
        s[0] = n;
        s[1] = n.wrapping_mul(7);
        s
    }

    fn onchain(ns: &[u8]) -> AHashSet<SigKey> {
        ns.iter().map(|&n| sig_key(&sig(n))).collect()
    }

    fn source(delivered: &[u8], duplicated: u64) -> SourceSlot {
        SourceSlot {
            sigs: delivered.iter().map(|&n| sig_key(&sig(n))).collect(),
            duplicated,
        }
    }

    fn audit_of(sources: &[SourceSlot], block: &[u8]) -> OnchainAudit {
        let mut audit = OnchainAudit::new(sources.len());
        compare_slot(&onchain(block), sources, &mut audit);
        audit
    }

    #[test]
    fn a_faithful_source_has_no_discrepancies() {
        let a = audit_of(&[source(&[1, 2, 3], 0)], &[1, 2, 3]);
        assert_eq!(a.sources[0].bad(), 0);
        assert_eq!(a.sources[0].onchain_txns, 3);
        assert_eq!(a.sources[0].slots_checked, 1);
        assert_eq!(a.sources[0].bad_fraction(), Some(0.0));
    }

    #[test]
    fn missing_extra_and_repeated_deliveries_are_counted_separately() {
        // block has 1,2,3,4; source delivered 1,2 (missed 3,4), plus 9 which is
        // not in the block at all, and repeated one signature twice.
        let a = audit_of(&[source(&[1, 2, 9], 1)], &[1, 2, 3, 4]);
        let s = a.sources[0];
        assert_eq!(s.missed, 2);
        assert_eq!(s.corrupted, 1);
        assert_eq!(s.duplicated, 1);
        assert_eq!(s.bad(), 4);
        assert_eq!(s.onchain_txns, 4);
        assert_eq!(s.bad_fraction(), Some(1.0));
    }

    #[test]
    fn a_source_absent_for_the_slot_is_not_scored() {
        let a = audit_of(&[source(&[], 0), source(&[1], 0)], &[1, 2]);
        assert_eq!(a.sources[0].slots_absent, 1);
        assert_eq!(a.sources[0].slots_checked, 0);
        assert_eq!(a.sources[0].onchain_txns, 0);
        assert_eq!(a.sources[0].bad_fraction(), None, "never scored");
        // its peer, which did deliver, is scored normally
        assert_eq!(a.sources[1].slots_checked, 1);
        assert_eq!(a.sources[1].missed, 1);
    }

    #[test]
    fn rates_accumulate_across_sampled_slots() {
        let mut a = OnchainAudit::new(1);
        compare_slot(&onchain(&[1, 2]), &[source(&[1, 2], 0)], &mut a);
        compare_slot(&onchain(&[3, 4]), &[source(&[3], 0)], &mut a);
        assert_eq!(a.slots_checked, 2);
        assert_eq!(a.sources[0].onchain_txns, 4);
        assert_eq!(a.sources[0].missed, 1);
        assert_eq!(a.sources[0].bad_fraction(), Some(0.25));
    }

    #[test]
    fn index_counts_repeat_deliveries_and_keeps_sources_apart() {
        let mut idx = SlotSigIndex::disabled();
        idx.enable(2, 32);
        idx.record(0, 100, &sig(1));
        idx.record(0, 100, &sig(1)); // repeat on the same source and slot
        idx.record(1, 100, &sig(1)); // a different source: not a duplicate
        let taken = idx.take(100).unwrap();
        assert_eq!(taken[0].sigs.len(), 1);
        assert_eq!(taken[0].duplicated, 1);
        assert_eq!(taken[1].duplicated, 0);
        assert!(idx.take(100).is_none(), "claimed slots are removed");
    }

    #[test]
    fn the_same_signature_in_two_slots_is_not_a_duplicate() {
        let mut idx = SlotSigIndex::disabled();
        idx.enable(1, 32);
        idx.record(0, 100, &sig(1));
        idx.record(0, 101, &sig(1));
        assert_eq!(idx.take(100).unwrap()[0].duplicated, 0);
        assert_eq!(idx.take(101).unwrap()[0].duplicated, 0);
    }

    #[test]
    fn the_window_stays_bounded_as_the_tip_advances() {
        let mut idx = SlotSigIndex::disabled();
        idx.enable(1, 8);
        for slot in 1..=100u64 {
            idx.record(0, slot, &sig(1));
        }
        assert_eq!(idx.tip(), 100);
        assert!(idx.slots.len() <= 9, "held {} slots", idx.slots.len());
        assert!(idx.take(100).is_some(), "the recent tail is retained");
        assert!(idx.take(50).is_none(), "old slots are pruned");
    }

    #[test]
    fn a_slot_already_behind_the_window_is_dropped_not_stored() {
        let mut idx = SlotSigIndex::disabled();
        idx.enable(1, 8);
        idx.record(0, 100, &sig(1));
        idx.record(0, 10, &sig(2)); // a very late straggler
        assert!(idx.take(10).is_none());
    }

    #[test]
    fn a_disabled_index_records_nothing() {
        let mut idx = SlotSigIndex::disabled();
        idx.record(0, 100, &sig(1));
        assert!(idx.take(100).is_none());
        assert_eq!(idx.tip(), 0);
    }

    #[test]
    fn skipped_slots_are_not_rpc_errors() {
        assert!(is_no_block(RPC_SLOT_SKIPPED));
        assert!(is_no_block(RPC_LONG_TERM_STORAGE_SLOT_SKIPPED));
        assert!(is_no_block(RPC_BLOCK_NOT_AVAILABLE));
        assert!(is_no_block(RPC_BLOCK_CLEANED_UP));
        assert!(!is_no_block(-32005), "node behind is a real failure");
        assert!(!is_no_block(429));
    }

    /// Pins what the key does and does not look at. Real signatures differ in the
    /// `R` point that starts at byte 0, so the prefix separates them; anything
    /// contrived to differ only past byte 16 is deliberately out of scope.
    #[test]
    fn sig_key_is_the_leading_16_bytes() {
        let mut a = [7u8; 64];
        let mut b = [7u8; 64];
        a[3] = 1;
        b[3] = 2;
        assert_ne!(sig_key(&a), sig_key(&b), "differ inside the prefix");

        let mut tail = [7u8; 64];
        tail[40] = 9;
        assert_eq!(
            sig_key(&[7u8; 64]),
            sig_key(&tail),
            "only the prefix is read — see the type's docs for why that is sound"
        );
    }
}
