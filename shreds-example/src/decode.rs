//! Accumulate shreds per slot, deshred completed ranges, and scan the decoded
//! transactions for the watched wallet.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use solana_entry::entry::{Entry, MaxDataShredsLen};
use solana_ledger::shred::{Shred, Shredder};
use solana_sdk::pubkey::Pubkey;
use wincode::{containers::Vec as WincodeVec, Deserialize as WincodeDeserialize};

use crate::receiver::ShredPacket;

/// Accumulated shreds for one slot, plus which data-complete ranges we've
/// already deshredded so we don't reprocess them.
pub struct SlotState {
    data_shreds: HashMap<u32, Shred>,
    complete_indices: Vec<u32>,
    processed_ends: HashSet<u32>,
    pub last_received: Instant,
}

impl SlotState {
    fn new(now: Instant) -> Self {
        Self {
            data_shreds: HashMap::new(),
            complete_indices: Vec::new(),
            processed_ends: HashSet::new(),
            last_received: now,
        }
    }
}

/// Decode a shred into its slot state and eagerly deshred any completed range,
/// returning new `(trigger_slot, trigger_signature)` matches for the watched
/// wallet (deduped via `seen_triggers`).
pub fn ingest_shred(
    slots: &mut HashMap<u64, SlotState>,
    packet: &ShredPacket,
    watch_wallet: &Pubkey,
    seen_triggers: &mut HashSet<String>,
) -> Vec<(u64, String)> {
    let shred = match Shred::new_from_serialized_shred(packet.data.clone()) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    if !shred.is_data() {
        return Vec::new();
    }

    let slot = shred.slot();
    let index = shred.index();
    let is_complete = shred.data_complete();

    let state = slots
        .entry(slot)
        .or_insert_with(|| SlotState::new(packet.received_at));
    state.last_received = packet.received_at;
    state.data_shreds.insert(index, shred);
    if is_complete && !state.complete_indices.contains(&index) {
        state.complete_indices.push(index);
    }

    process_ready_ranges(state, slot, watch_wallet, seen_triggers)
}

/// Try to deshred every data-complete range that is now fully present and not
/// yet processed, scanning each for the watched wallet.
fn process_ready_ranges(
    state: &mut SlotState,
    slot: u64,
    watch_wallet: &Pubkey,
    seen_triggers: &mut HashSet<String>,
) -> Vec<(u64, String)> {
    let mut hits = Vec::new();

    let mut boundaries = state.complete_indices.clone();
    boundaries.sort_unstable();

    for &end in &boundaries {
        if state.processed_ends.contains(&end) {
            continue;
        }
        // Start = one past the previous data-complete boundary, else 0.
        let start = boundaries
            .iter()
            .filter(|&&b| b < end)
            .max()
            .map(|&b| b + 1)
            .unwrap_or(0);

        let mut shreds: Vec<&Shred> = Vec::with_capacity((end - start + 1) as usize);
        let mut gap = false;
        for idx in start..=end {
            match state.data_shreds.get(&idx) {
                Some(s) => shreds.push(s),
                None => {
                    gap = true;
                    break;
                }
            }
        }
        if gap {
            continue;
        }

        state.processed_ends.insert(end);

        let payloads: Vec<&[u8]> = shreds.iter().map(|s| s.payload().as_ref()).collect();
        let payload = match Shredder::deshred(payloads) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let entries =
            match <WincodeVec<Entry, MaxDataShredsLen> as WincodeDeserialize>::deserialize(&payload)
            {
                Ok(e) => e,
                Err(_) => continue,
            };

        // Scan every decoded transaction for the watched wallet. `entries` is
        // consumed by value here because wincode's `Vec` only yields items when
        // owned (not through a shared reference), so this stays inline.
        let target = watch_wallet.to_string();
        for entry in entries.iter() {
            for txn in entry.transactions.iter() {
                let touches = txn
                    .message
                    .static_account_keys()
                    .iter()
                    .any(|k| k.to_string() == target);
                if !touches {
                    continue;
                }
                let sig = txn
                    .signatures
                    .first()
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                if sig.is_empty() {
                    continue;
                }
                if seen_triggers.insert(sig.clone()) {
                    hits.push((slot, sig));
                }
            }
        }
    }

    hits
}
