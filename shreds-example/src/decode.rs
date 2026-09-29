use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Instant;

use solana_entry::entry::{Entry, MaxDataShredsLen};
use solana_ledger::shred::{Shred, Shredder};
use solana_sdk::pubkey::Pubkey;
use wincode::{containers::Vec as WincodeVec, Deserialize as WincodeDeserialize};

use crate::receiver::ShredPacket;

pub struct SlotState {
    data_shreds: HashMap<u32, Shred>,
    complete_indices: BTreeSet<u32>,
    processed_ends: HashSet<u32>,
    pub last_received: Instant,
}

/// Store a data shred and deshred every data-complete range that is now fully
/// present, returning new `(slot, signature)` txs touching `watch_wallet`.
pub fn ingest_shred(
    slots: &mut HashMap<u64, SlotState>,
    packet: ShredPacket,
    watch_wallet: &Pubkey,
    seen_triggers: &mut HashSet<String>,
) -> Vec<(u64, String)> {
    let Ok(shred) = Shred::new_from_serialized_shred(packet.data) else {
        return Vec::new();
    };
    if !shred.is_data() {
        return Vec::new();
    }

    let slot = shred.slot();
    let state = slots.entry(slot).or_insert_with(|| SlotState {
        data_shreds: HashMap::new(),
        complete_indices: BTreeSet::new(),
        processed_ends: HashSet::new(),
        last_received: packet.received_at,
    });
    state.last_received = packet.received_at;
    if shred.data_complete() {
        state.complete_indices.insert(shred.index());
    }
    state.data_shreds.insert(shred.index(), shred);

    let mut hits = Vec::new();
    let mut start = 0;
    for &end in &state.complete_indices {
        let range = start..=end;
        start = end + 1;
        if state.processed_ends.contains(&end) {
            continue;
        }
        let Some(payloads) = range
            .map(|i| state.data_shreds.get(&i).map(|s| s.payload().as_ref()))
            .collect::<Option<Vec<&[u8]>>>()
        else {
            continue;
        };
        state.processed_ends.insert(end);

        let Ok(payload) = Shredder::deshred(payloads) else {
            continue;
        };
        let Ok(entries) =
            <WincodeVec<Entry, MaxDataShredsLen> as WincodeDeserialize>::deserialize(&payload)
        else {
            continue;
        };

        for txn in entries.iter().flat_map(|entry| entry.transactions.iter()) {
            if !txn.message.static_account_keys().contains(watch_wallet) {
                continue;
            }
            let Some(sig) = txn.signatures.first() else {
                continue;
            };
            let sig = sig.to_string();
            if seen_triggers.insert(sig.clone()) {
                hits.push((slot, sig));
            }
        }
    }
    hits
}
