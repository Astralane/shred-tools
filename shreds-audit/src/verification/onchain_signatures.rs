//! Samples settled slots via `getBlock` and diffs each source's deliveries against the block.

use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::rpc::{RpcEndpoint, RpcError};
use crate::sigreg::SigRegistry;

/// Leading 16 bytes of the signature: part of the ed25519 `R` point, unique per transaction.
type SigKey = u128;

fn sig_key(sig: &[u8; 64]) -> SigKey {
    let mut head = [0u8; 16];
    head.copy_from_slice(&sig[..16]);
    u128::from_le_bytes(head)
}

/// Slots are claimed before the RPC call, so this only absorbs jitter in source delivery.
const RETAIN_MARGIN_SLOTS: u64 = 16;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const SLEEP_STEP: Duration = Duration::from_millis(100);

// "No block here" codes: leaders skip slots, so these are not RPC errors.
const RPC_BLOCK_CLEANED_UP: i64 = -32001;
const RPC_BLOCK_NOT_AVAILABLE: i64 = -32004;
const RPC_SLOT_SKIPPED: i64 = -32007;
const RPC_LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;

#[derive(Default)]
pub struct SourceSlot {
    sigs: AHashSet<SigKey>,
    duplicated: u64,
}

impl SourceSlot {
    fn is_empty(&self) -> bool {
        self.sigs.is_empty() && self.duplicated == 0
    }
}

/// Per-slot, per-source signature sets, holding only slots within `retain_slots` of the tip.
pub struct SlotSigIndex {
    n_sources: usize,
    slots: AHashMap<u64, Vec<SourceSlot>>,
    tip: u64,
    retain_slots: u64,
}

impl SlotSigIndex {
    pub fn new(n_sources: usize, retain_slots: u64) -> Self {
        Self {
            n_sources,
            slots: AHashMap::new(),
            tip: 0,
            retain_slots,
        }
    }

    pub fn tip(&self) -> u64 {
        self.tip
    }

    pub fn record(&mut self, sid: usize, slot: u64, sig: &[u8; 64]) {
        if slot == 0 {
            return;
        }
        if slot > self.tip {
            self.tip = slot;
            let floor = self.tip.saturating_sub(self.retain_slots);
            self.slots.retain(|&s, _| s >= floor);
        }
        if slot < self.tip.saturating_sub(self.retain_slots) {
            return;
        }
        let n = self.n_sources;
        let sources = self
            .slots
            .entry(slot)
            .or_insert_with(|| (0..n).map(|_| SourceSlot::default()).collect());
        let s = &mut sources[sid];
        if !s.sigs.insert(sig_key(sig)) {
            s.duplicated += 1;
        }
    }

    /// A straggler for a taken slot re-creates an entry that simply ages out.
    pub fn take(&mut self, slot: u64) -> Option<Vec<SourceSlot>> {
        self.slots.remove(&slot)
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct SourceAudit {
    pub slots_checked: u64,
    /// Sampled slots this source delivered nothing for; not scored, so a disconnect is not corruption.
    pub slots_absent: u64,
    pub onchain_txns: u64,
    pub missed: u64,
    pub corrupted: u64,
    pub duplicated: u64,
}

impl SourceAudit {
    pub fn bad(&self) -> u64 {
        self.missed + self.corrupted + self.duplicated
    }

    pub fn bad_fraction(&self) -> Option<f64> {
        (self.onchain_txns > 0).then(|| self.bad() as f64 / self.onchain_txns as f64)
    }
}

#[derive(Clone, Default)]
pub struct OnchainAudit {
    pub sources: Vec<SourceAudit>,
    pub slots_checked: u64,
    pub slots_unavailable: u64,
    pub rpc_errors: u64,
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

pub struct OnchainVerifier {
    audit: Arc<Mutex<OnchainAudit>>,
    handle: Option<JoinHandle<()>>,
}

impl OnchainVerifier {
    pub fn start(
        rpc: RpcEndpoint,
        lag_slots: u64,
        sample_secs: u64,
        n_sources: usize,
        reg: Arc<Mutex<SigRegistry>>,
        cancel: CancellationToken,
    ) -> Self {
        reg.lock()
            .unwrap()
            .enable_onchain_index(lag_slots + RETAIN_MARGIN_SLOTS, rpc.omits_votes());
        let audit = Arc::new(Mutex::new(OnchainAudit::new(n_sources)));
        let worker = Worker {
            rpc,
            lag_slots,
            tick: Duration::from_secs(sample_secs),
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

    pub fn snapshot(&self) -> OnchainAudit {
        self.audit.lock().unwrap().clone()
    }

    pub fn finish(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

struct Worker {
    rpc: RpcEndpoint,
    lag_slots: u64,
    tick: Duration,
    reg: Arc<Mutex<SigRegistry>>,
    audit: Arc<Mutex<OnchainAudit>>,
    last_sampled: u64,
    announced_error: bool,
}

impl Worker {
    fn run(mut self, cancel: CancellationToken) {
        while wait_for_tick(&cancel, self.tick) {
            self.sample(&cancel);
        }
    }

    fn sample(&mut self, cancel: &CancellationToken) {
        // The registry lock guards the hot path, so it is released before the RPC call.
        let (slot, sources, skip) = {
            let mut reg = self.reg.lock().unwrap();
            let target = reg.onchain_tip().saturating_sub(self.lag_slots);
            if target == 0 || target <= self.last_sampled {
                return;
            }
            self.last_sampled = target;
            let Some(sources) = reg.take_onchain_slot(target) else {
                return;
            };
            let skip: Vec<bool> = (0..sources.len()).map(|i| reg.is_rotating(i)).collect();
            (target, sources, skip)
        };
        if cancel.is_cancelled() {
            return;
        }

        let onchain = match block_signatures(&self.rpc, slot) {
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
        compare_slot(&onchain, &sources, &skip, &mut self.audit.lock().unwrap());
    }
}

fn compare_slot(
    onchain: &AHashSet<SigKey>,
    sources: &[SourceSlot],
    skip: &[bool],
    audit: &mut OnchainAudit,
) {
    audit.slots_checked += 1;
    for (sid, s) in sources.iter().enumerate() {
        if skip.get(sid).copied().unwrap_or(false) {
            continue;
        }
        let a = &mut audit.sources[sid];
        if s.is_empty() {
            a.slots_absent += 1;
            continue;
        }
        let hit = s.sigs.iter().filter(|k| onchain.contains(k)).count() as u64;
        a.slots_checked += 1;
        a.onchain_txns += onchain.len() as u64;
        a.missed += onchain.len() as u64 - hit;
        a.corrupted += s.sigs.len() as u64 - hit;
        a.duplicated += s.duplicated;
    }
}

/// Returns `false` once cancelled; sleeps in short steps so shutdown stays prompt.
fn wait_for_tick(cancel: &CancellationToken, tick: Duration) -> bool {
    let deadline = Instant::now() + tick;
    while !cancel.is_cancelled() {
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        std::thread::sleep(SLEEP_STEP.min(deadline - now));
    }
    false
}

#[derive(Deserialize)]
struct BlockSignatures {
    #[serde(default)]
    signatures: Vec<String>,
}

/// `None` when the cluster produced no block for `slot`.
fn block_signatures(rpc: &RpcEndpoint, slot: u64) -> Result<Option<AHashSet<SigKey>>> {
    let params = serde_json::json!([slot, {
        "encoding": "json",
        "transactionDetails": "signatures",
        "rewards": false,
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 1,
    }]);
    let block: BlockSignatures = match rpc.call("getBlock", params, RPC_TIMEOUT) {
        Ok(b) => b,
        Err(e) if e.downcast_ref::<RpcError>().is_some_and(|r| is_no_block(r.code)) => {
            return Ok(None)
        }
        Err(e) => return Err(e),
    };

    let mut out = AHashSet::with_capacity(block.signatures.len());
    for s in &block.signatures {
        // Fail the whole sample: skipping would shrink the denominator and hide misses.
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
        compare_slot(&onchain(block), sources, &[], &mut audit);
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
        compare_slot(&onchain(&[1, 2]), &[source(&[1, 2], 0)], &[], &mut a);
        compare_slot(&onchain(&[3, 4]), &[source(&[3], 0)], &[], &mut a);
        assert_eq!(a.slots_checked, 2);
        assert_eq!(a.sources[0].onchain_txns, 4);
        assert_eq!(a.sources[0].missed, 1);
        assert_eq!(a.sources[0].bad_fraction(), Some(0.25));
    }

    #[test]
    fn index_counts_repeat_deliveries_and_keeps_sources_apart() {
        let mut idx = SlotSigIndex::new(2, 32);
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
        let mut idx = SlotSigIndex::new(1, 32);
        idx.record(0, 100, &sig(1));
        idx.record(0, 101, &sig(1));
        assert_eq!(idx.take(100).unwrap()[0].duplicated, 0);
        assert_eq!(idx.take(101).unwrap()[0].duplicated, 0);
    }

    #[test]
    fn the_window_stays_bounded_as_the_tip_advances() {
        let mut idx = SlotSigIndex::new(1, 8);
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
        let mut idx = SlotSigIndex::new(1, 8);
        idx.record(0, 100, &sig(1));
        idx.record(0, 10, &sig(2)); // a very late straggler
        assert!(idx.take(10).is_none());
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
            "only the prefix is read"
        );
    }
}
