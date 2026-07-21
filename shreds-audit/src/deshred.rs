//! Reconstruct transactions from the shred stream and time their arrival.
//!
//! The FEC-set path in `agg.rs` answers "when did a set become decodable"; this
//! path goes one step further and answers "when did a specific transaction first
//! become readable from shreds". It buffers data shreds per slot, and once a slot
//! has been quiet for a settle interval it reconstructs every complete entry batch
//! (a contiguous run of data shreds ending on a DATA_COMPLETE boundary),
//! deserializes the entries, and reports each transaction's first signature to the
//! shared signature registry, stamped with the arrival of the last shred that
//! completed its batch — the moment the transaction became readable.
//!
//! Coding shreds are not used for recovery here: a batch with any missing data
//! shred is skipped. Erasure recovery would let a partially-delivered batch still
//! be read, but it is not required to time the common case and is left for later.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crossbeam_channel::Receiver;
use solana_entry::entry::{Entry, MaxDataShredsLen};
use solana_ledger::shred::{Shred, Shredder};
use wincode::{containers::Vec as WincodeVec, Deserialize};

use crate::sigreg::SigRegistry;

/// One data-shred payload handed to the deshredder, with its kernel arrival time
/// and the provider that delivered it. Each provider's shreds are reconstructed
/// independently, so a transaction is timed per provider — the same provider
/// that raced the FEC-set decode also races the transaction here.
pub struct ShredInput {
    /// CLOCK_REALTIME nanoseconds, stamped by the kernel at driver handoff.
    pub rx_unix_ns: i64,
    /// Provider id, which is also this source's id in the registry.
    pub provider: u16,
    pub data: Vec<u8>,
}

struct BufferedShred {
    shred: Shred,
    rx_unix_ns: i64,
}

struct SlotBuffer {
    /// data shred index -> buffered shred
    data: HashMap<u32, BufferedShred>,
    /// indices carrying the DATA_COMPLETE flag (entry-batch boundaries)
    complete: Vec<u32>,
    /// Local monotonic time of the last shred for this slot, for settle timing.
    last_seen: Instant,
    settled: bool,
}

pub struct Deshredder {
    reg: Arc<Mutex<SigRegistry>>,
    /// (provider, slot) -> buffered data shreds for that provider's stream.
    slots: HashMap<(u16, u64), SlotBuffer>,
    settle: Duration,
}

impl Deshredder {
    pub fn new(reg: Arc<Mutex<SigRegistry>>, settle: Duration) -> Self {
        Self {
            reg,
            slots: HashMap::new(),
            settle,
        }
    }

    /// Consume shred inputs until the channel closes, sweeping settled slots once a
    /// second. A final sweep on shutdown flushes whatever is still buffered.
    pub fn run(mut self, rx: Receiver<ShredInput>) {
        let mut last_sweep = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(input) => self.ingest(input),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
            if last_sweep.elapsed() >= Duration::from_secs(1) {
                self.sweep();
                last_sweep = Instant::now();
            }
        }
        // Force everything out on shutdown.
        let keys: Vec<(u16, u64)> = self.slots.keys().copied().collect();
        for key in keys {
            self.deshred_slot(key);
        }
    }

    fn ingest(&mut self, input: ShredInput) {
        let provider = input.provider;
        let Ok(shred) = Shred::new_from_serialized_shred(input.data) else {
            return;
        };
        if !shred.is_data() {
            return;
        }
        let slot = shred.slot();
        let index = shred.index();
        let is_complete = shred.data_complete();

        let buf = self.slots.entry((provider, slot)).or_insert_with(|| SlotBuffer {
            data: HashMap::new(),
            complete: Vec::new(),
            last_seen: Instant::now(),
            settled: false,
        });
        buf.last_seen = Instant::now();
        buf.data.entry(index).or_insert(BufferedShred {
            shred,
            rx_unix_ns: input.rx_unix_ns,
        });
        if is_complete && !buf.complete.contains(&index) {
            buf.complete.push(index);
        }
    }

    fn sweep(&mut self) {
        let now = Instant::now();
        let settle = self.settle;
        // Retire slots reconstructed on a previous sweep so the map stays bounded
        // over a run of any length. A late straggler for a retired slot simply
        // re-creates a fresh buffer and is timed again (harmless: `record_first`
        // dedupes by signature), which keeps the common path allocation-free.
        self.slots.retain(|_, b| !b.settled);
        let ready: Vec<(u16, u64)> = self
            .slots
            .iter()
            .filter(|(_, b)| !b.settled && now.duration_since(b.last_seen) >= settle)
            .map(|(&k, _)| k)
            .collect();
        for key in ready {
            self.deshred_slot(key);
        }
    }

    fn deshred_slot(&mut self, key: (u16, u64)) {
        let (provider, slot) = key;
        let Some(buf) = self.slots.get_mut(&key) else {
            return;
        };
        buf.settled = true;
        let mut boundaries = buf.complete.clone();
        boundaries.sort_unstable();

        let mut hits: Vec<([u8; 64], i64)> = Vec::new();
        for end in boundaries {
            // A batch runs from one past the previous complete boundary (or 0) up
            // to and including this one.
            let start = buf
                .complete
                .iter()
                .copied()
                .filter(|&i| i < end)
                .max()
                .map(|i| i + 1)
                .unwrap_or(0);

            let mut shreds: Vec<&Shred> = Vec::new();
            let mut batch_ns = i64::MIN;
            let mut gap = false;
            for idx in start..=end {
                match buf.data.get(&idx) {
                    Some(b) => {
                        shreds.push(&b.shred);
                        batch_ns = batch_ns.max(b.rx_unix_ns);
                    }
                    None => {
                        gap = true;
                        break;
                    }
                }
            }
            if gap || shreds.is_empty() {
                continue;
            }

            let payloads: Vec<&[u8]> = shreds.iter().map(|s| s.payload().as_ref()).collect();
            let Ok(payload) = Shredder::deshred(payloads) else {
                continue;
            };
            let Ok(entries) =
                <WincodeVec<Entry, MaxDataShredsLen> as Deserialize>::deserialize(&payload)
            else {
                continue;
            };
            for entry in entries.iter() {
                for tx in entry.transactions.iter() {
                    if let Some(sig0) = tx.signatures.first() {
                        let bytes: &[u8] = sig0.as_ref();
                        if bytes.len() >= 64 {
                            let mut sig = [0u8; 64];
                            sig.copy_from_slice(&bytes[..64]);
                            hits.push((sig, batch_ns));
                        }
                    }
                }
            }
        }

        if !hits.is_empty() {
            let mut reg = self.reg.lock().unwrap();
            for (sig, ns) in hits {
                reg.record_first(provider as usize, sig, ns, slot);
            }
        }
    }
}

/// The variant byte's top nibble marks a data shred. Used to cheaply skip coding
/// shreds and non-shred datagrams before cloning a payload into the deshred feed.
pub fn is_data_shred_variant(first_payload_byte_at_variant: u8) -> bool {
    matches!(first_payload_byte_at_variant & 0xf0, 0x80 | 0x90 | 0xb0)
}
