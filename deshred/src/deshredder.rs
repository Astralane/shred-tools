use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::{Duration, Instant},
};

use bytes::Bytes;
use solana_entry::entry::{Entry, MaxDataShredsLen};
use solana_ledger::shred::{ReedSolomonCache, Shred, Shredder, recover};
use solana_transaction::versioned::VersionedTransaction;
use wincode::{Deserialize, containers::Vec as WincodeVec};

const MAX_RECOVERY_ATTEMPTS: u8 = 3;
const SIZE_OF_COMMON_SHRED_HEADER: usize = 83;
const MAX_SLOTS_AHEAD: u64 = 64;
const RESYNC_AFTER_REJECTED: u32 = 1_024;
const WARMUP_SHREDS: usize = 64;

#[derive(Debug, Clone)]
pub struct CompletedDataSet {
    pub slot: u64,
    pub start: u32,
    pub end: u32,
    pub entries: Vec<Entry>,
}

impl CompletedDataSet {
    pub fn transactions(&self) -> impl Iterator<Item = &VersionedTransaction> {
        self.entries
            .iter()
            .flat_map(|entry| entry.transactions.iter())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryStats {
    pub elapsed: Duration,
    pub recovered: usize,
}

#[derive(Debug, Default)]
pub struct ShredInsertionResult {
    pub data_sets: Vec<CompletedDataSet>,
    pub recovery: Option<RecoveryStats>,
    pub decode_errors: usize,
}

pub struct Deshredder {
    max_wait_slots: u64,
    rs_cache: ReedSolomonCache,
    slots: HashMap<u64, SlotState>,
    max_slot: Option<u64>,
    warmup: Vec<u64>,
    rejected_in_a_row: u32,
}

impl Deshredder {
    pub fn new(max_wait_slots: u64) -> Self {
        Self {
            max_wait_slots,
            rs_cache: ReedSolomonCache::default(),
            slots: HashMap::new(),
            max_slot: None,
            warmup: Vec::new(),
            rejected_in_a_row: 0,
        }
    }

    pub fn pending_slots(&self) -> usize {
        self.slots.len()
    }

    pub fn insert(&mut self, payload: Bytes) -> ShredInsertionResult {
        match Shred::new_from_serialized_shred(payload) {
            Ok(shred) => self.insert_shred(shred),
            Err(_) => ShredInsertionResult::default(),
        }
    }

    fn insert_shred(&mut self, shred: Shred) -> ShredInsertionResult {
        let slot = shred.slot();
        if !self.accept_slot(slot) {
            return ShredInsertionResult::default();
        }

        let index = shred.index();
        let fec = shred.fec_set_index();
        let state = self.slots.entry(slot).or_insert_with(SlotState::new);
        let mut changed = None;
        if shred.is_data() {
            if state.insert_data(shred) {
                changed = Some((index, index));
            }
        } else {
            state.insert_coding(shred);
        }

        let recovery = state.recover(fec, &self.rs_cache);
        if let Some((num_data, stats)) = recovery
            && stats.recovered > 0
        {
            changed = Some((fec, fec + num_data - 1));
        }

        let mut result = ShredInsertionResult {
            recovery: recovery.map(|(_, stats)| stats),
            ..ShredInsertionResult::default()
        };
        if let Some((first, last)) = changed {
            for data_set in state.complete_data_sets(slot, first, last) {
                match data_set {
                    Some(data_set) => result.data_sets.push(data_set),
                    None => result.decode_errors += 1,
                }
            }
        }
        result
    }

    fn accept_slot(&mut self, slot: u64) -> bool {
        let Some(max_slot) = self.max_slot else {
            self.warm_up(slot);
            return true;
        };
        let too_old = max_slot.saturating_sub(slot) >= self.max_wait_slots;
        let too_new = slot > max_slot.saturating_add(MAX_SLOTS_AHEAD);
        if too_old || too_new {
            self.rejected_in_a_row += 1;
            if self.rejected_in_a_row < RESYNC_AFTER_REJECTED {
                return false;
            }
            self.rejected_in_a_row = 0;
            self.max_slot = None;
            self.warm_up(slot);
            return true;
        }
        self.rejected_in_a_row = 0;
        if slot > max_slot {
            self.max_slot = Some(slot);
            self.evict(slot);
        }
        true
    }

    fn warm_up(&mut self, slot: u64) {
        self.warmup.push(slot);
        if self.warmup.len() < WARMUP_SHREDS {
            return;
        }
        let middle = self.warmup.len() / 2;
        let slot = *self.warmup.select_nth_unstable(middle).1;
        self.warmup.clear();
        self.max_slot = Some(slot);
        self.evict(slot);
    }

    fn evict(&mut self, max_slot: u64) {
        let (newest, oldest) = (
            max_slot.saturating_add(MAX_SLOTS_AHEAD),
            max_slot.saturating_sub(self.max_wait_slots - 1),
        );
        self.slots
            .retain(|&pending, _| (oldest..=newest).contains(&pending));
    }
}

struct CodingSet {
    num_data: u32,
    shreds: BTreeMap<u32, Shred>,
    attempts: u8,
}

struct SlotState {
    data: BTreeMap<u32, Shred>,
    batch_ends: BTreeSet<u32>,
    emitted: BTreeMap<u32, u32>,
    coding: HashMap<u32, CodingSet>,
}

impl SlotState {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            batch_ends: BTreeSet::new(),
            emitted: BTreeMap::new(),
            coding: HashMap::new(),
        }
    }

    fn is_emitted(&self, index: u32) -> bool {
        self.emitted
            .range(..=index)
            .next_back()
            .is_some_and(|(_, &end)| index <= end)
    }

    fn mark_emitted(&mut self, mut start: u32, mut end: u32) {
        if let Some((&prev_start, &prev_end)) = self.emitted.range(..start).next_back()
            && prev_end + 1 == start
        {
            self.emitted.remove(&prev_start);
            start = prev_start;
        }
        if let Some(next_end) = self.emitted.remove(&(end + 1)) {
            end = next_end;
        }
        self.emitted.insert(start, end);
    }

    fn insert_data(&mut self, shred: Shred) -> bool {
        let index = shred.index();
        if self.data.contains_key(&index) || self.is_emitted(index) {
            return false;
        }
        if shred.data_complete() {
            self.batch_ends.insert(index);
        }
        self.data.insert(index, shred);
        true
    }

    fn insert_coding(&mut self, shred: Shred) {
        let fec = shred.fec_set_index();
        let Some(num_data) = coding_num_data(shred.payload().as_ref()) else {
            return;
        };
        if self.missing(fec, num_data) == 0 {
            return;
        }
        self.coding
            .entry(fec)
            .or_insert_with(|| CodingSet {
                num_data,
                shreds: BTreeMap::new(),
                attempts: 0,
            })
            .shreds
            .entry(shred.index())
            .or_insert(shred);
    }

    fn missing(&self, fec: u32, num_data: u32) -> usize {
        (fec..fec + num_data)
            .filter(|index| !self.data.contains_key(index) && !self.is_emitted(*index))
            .count()
    }

    fn recover(&mut self, fec: u32, cache: &ReedSolomonCache) -> Option<(u32, RecoveryStats)> {
        let set = self.coding.get(&fec)?;
        let num_data = set.num_data;
        if self.missing(fec, num_data) == 0 {
            self.coding.remove(&fec);
            return None;
        }
        let available = self.data.range(fec..fec + num_data).count() + set.shreds.len();
        if available < num_data as usize || set.attempts >= MAX_RECOVERY_ATTEMPTS {
            return None;
        }

        let started = Instant::now();
        let input: Vec<Shred> = self
            .data
            .range(fec..fec + num_data)
            .map(|(_, shred)| shred.clone())
            .chain(set.shreds.values().cloned())
            .collect();
        self.coding.get_mut(&fec)?.attempts += 1;
        let recovered = recover(input, cache)
            .ok()?
            .flatten()
            .filter(|shred| shred.is_data() && self.insert_data(shred.clone()))
            .count();
        if self.missing(fec, num_data) == 0 {
            self.coding.remove(&fec);
        }
        Some((
            num_data,
            RecoveryStats {
                elapsed: started.elapsed(),
                recovered,
            },
        ))
    }

    fn complete_data_sets(
        &mut self,
        slot: u64,
        first: u32,
        last: u32,
    ) -> Vec<Option<CompletedDataSet>> {
        let mut start = self
            .batch_ends
            .range(..first)
            .next_back()
            .map_or(0, |end| end + 1);
        let mut ends = Vec::new();
        for &end in self.batch_ends.range(first..) {
            ends.push(end);
            if end > last {
                break;
            }
        }

        let mut data_sets = Vec::new();
        for end in ends {
            let complete = !self.is_emitted(start)
                && self.data.range(start..=end).count() == (end - start + 1) as usize;
            if complete {
                data_sets.push(self.take_data_set(slot, start, end));
            }
            start = end + 1;
        }
        data_sets
    }

    fn take_data_set(&mut self, slot: u64, start: u32, end: u32) -> Option<CompletedDataSet> {
        let mut rest = self.data.split_off(&(end + 1));
        let batch = self.data.split_off(&start);
        self.data.append(&mut rest);
        self.mark_emitted(start, end);
        let done: Vec<u32> = self
            .coding
            .iter()
            .filter(|&(&fec, set)| {
                fec <= end && fec + set.num_data > start && self.missing(fec, set.num_data) == 0
            })
            .map(|(&fec, _)| fec)
            .collect();
        for fec in done {
            self.coding.remove(&fec);
        }

        let bytes = Shredder::deshred(batch.values().map(Shred::payload)).ok()?;
        let entries =
            <WincodeVec<Entry, MaxDataShredsLen> as Deserialize>::deserialize(&bytes).ok()?;
        Some(CompletedDataSet {
            slot,
            start,
            end,
            entries,
        })
    }
}

fn coding_num_data(payload: &[u8]) -> Option<u32> {
    let bytes = payload.get(SIZE_OF_COMMON_SHRED_HEADER..SIZE_OF_COMMON_SHRED_HEADER + 2)?;
    let num_data = u16::from_le_bytes(bytes.try_into().ok()?);
    (num_data > 0).then_some(u32::from(num_data))
}

#[cfg(test)]
mod tests {
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_ledger::shred::{
        DATA_SHREDS_PER_FEC_BLOCK, ProcessShredsStats, SIZE_OF_DATA_SHRED_HEADERS,
    };

    use super::*;

    const SLOT: u64 = 42;

    fn entries(count: usize) -> Vec<Entry> {
        (0..count)
            .map(|_| Entry::new(&Hash::default(), 1, Vec::new()))
            .collect()
    }

    fn shred(entries: &[Entry], next_index: u32, last_in_slot: bool) -> (Vec<Shred>, Vec<Shred>) {
        shred_in_slot(SLOT, entries, next_index, last_in_slot)
    }

    fn shred_in_slot(
        slot: u64,
        entries: &[Entry],
        next_index: u32,
        last_in_slot: bool,
    ) -> (Vec<Shred>, Vec<Shred>) {
        Shredder::new(slot, slot - 1, 0, 42)
            .unwrap()
            .entries_to_merkle_shreds_for_tests(
                &Keypair::new(),
                entries,
                last_in_slot,
                Hash::default(),
                next_index,
                next_index,
                &ReedSolomonCache::default(),
                &mut ProcessShredsStats::default(),
            )
    }

    fn warm_up(deshredder: &mut Deshredder, shred: &Shred) {
        for _ in 0..WARMUP_SHREDS {
            deshredder.insert_shred(shred.clone());
        }
    }

    fn insert_all(
        deshredder: &mut Deshredder,
        shreds: impl IntoIterator<Item = Shred>,
    ) -> Vec<CompletedDataSet> {
        shreds
            .into_iter()
            .flat_map(|shred| deshredder.insert_shred(shred).data_sets)
            .collect()
    }

    #[test]
    fn coding_shreds_report_the_fec_sets_data_count() {
        let (_, coding) = shred(&entries(1_000), 0, false);
        assert_eq!(
            coding_num_data(coding[0].payload().as_ref()),
            Some(DATA_SHREDS_PER_FEC_BLOCK as u32)
        );
    }

    #[test]
    fn decodes_a_complete_data_set() {
        let entries = entries(1_000);
        let (data, _) = shred(&entries, 0, false);
        let last = data.last().unwrap().index();

        let completed = insert_all(&mut Deshredder::new(5), data);

        assert_eq!(completed.len(), 1);
        assert_eq!(
            (completed[0].slot, completed[0].start, completed[0].end),
            (SLOT, 0, last)
        );
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn recovers_a_missing_data_shred_from_coding_shreds() {
        let entries = entries(1_000);
        let (data, coding) = shred(&entries, 0, false);
        let dropped = &data[DATA_SHREDS_PER_FEC_BLOCK / 2];
        let (dropped_index, dropped_fec) = (dropped.index(), dropped.fec_set_index());
        let mut deshredder = Deshredder::new(5);

        let without_dropped = data.into_iter().filter(|s| s.index() != dropped_index);
        assert!(insert_all(&mut deshredder, without_dropped).is_empty());

        let fec_coding = coding
            .into_iter()
            .filter(|s| s.fec_set_index() == dropped_fec);
        let completed = insert_all(&mut deshredder, fec_coding);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn reports_the_recovery() {
        let (data, coding) = shred(&entries(1_000), 0, false);
        let fec = data[0].fec_set_index();
        let mut deshredder = Deshredder::new(5);
        insert_all(&mut deshredder, data.into_iter().skip(1));

        let recoveries: Vec<RecoveryStats> = coding
            .into_iter()
            .filter(|s| s.fec_set_index() == fec)
            .filter_map(|s| deshredder.insert_shred(s).recovery)
            .collect();
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].recovered, 1);
    }

    #[test]
    fn recovery_only_counts_data_shreds_still_held() {
        let (data, coding) = shred(&entries(1_000), 0, false);
        let fec = data[0].fec_set_index();
        let num_data = DATA_SHREDS_PER_FEC_BLOCK as u32;
        let missing = fec + num_data - 1;
        let mut state = SlotState::new();
        state.mark_emitted(fec, fec + num_data / 2 - 1);
        for shred in data.into_iter().filter(|s| s.index() != missing) {
            state.insert_data(shred);
        }
        let mut coding = coding.into_iter().filter(|s| s.fec_set_index() == fec);
        let cache = ReedSolomonCache::default();

        state.insert_coding(coding.next().unwrap());
        assert!(state.recover(fec, &cache).is_none());
        assert_eq!(state.coding[&fec].attempts, 0);

        let held = state.data.range(fec..fec + num_data).count() + 1;
        for shred in coding.take(num_data as usize - held) {
            state.insert_coding(shred);
        }
        let (_, recovery) = state.recover(fec, &cache).unwrap();
        assert_eq!(recovery.recovered, 1);
        assert!(state.data.contains_key(&missing));
        assert!(!state.coding.contains_key(&fec));
    }

    #[test]
    fn undecodable_data_set_does_not_block_the_next_one() {
        let entries = entries(10);
        let (mut first, _) = shred(&entries, 0, false);
        let next = first.last().unwrap().index() + 1;
        let (second, _) = shred(&entries, next, true);
        let mut payload = first[0].payload().to_vec();
        payload[SIZE_OF_DATA_SHRED_HEADERS..][..8].fill(0xff);
        first[0] = Shred::new_from_serialized_shred(payload).unwrap();

        let mut deshredder = Deshredder::new(5);
        let results: Vec<ShredInsertionResult> = first
            .into_iter()
            .chain(second)
            .map(|s| deshredder.insert_shred(s))
            .collect();

        assert_eq!(results.iter().map(|r| r.decode_errors).sum::<usize>(), 1);
        let completed: Vec<_> = results.into_iter().flat_map(|r| r.data_sets).collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start, next);
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn drops_slots_left_behind_by_the_stream() {
        let mut deshredder = Deshredder::new(2);
        let (old, _) = shred(&entries(10), 0, false);
        warm_up(&mut deshredder, &old[0]);
        assert_eq!(deshredder.pending_slots(), 1);

        let (new, _) = shred_in_slot(SLOT + 2, &entries(10), 0, false);
        deshredder.insert_shred(new[0].clone());
        assert_eq!(deshredder.pending_slots(), 1);

        deshredder.insert_shred(old[1].clone());
        assert_eq!(deshredder.pending_slots(), 1);
    }

    #[test]
    fn a_later_data_set_does_not_wait_for_an_earlier_one() {
        let entries = entries(10);
        let (first, _) = shred(&entries, 0, false);
        let next = first.last().unwrap().index() + 1;
        let (second, _) = shred(&entries, next, true);
        let lost = first[0].clone();
        let mut deshredder = Deshredder::new(5);

        let completed = insert_all(&mut deshredder, first.into_iter().skip(1).chain(second));
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start, next);

        let completed = insert_all(&mut deshredder, [lost.clone(), lost]);
        assert_eq!(completed.len(), 1);
        assert_eq!((completed[0].start, completed[0].end), (0, next - 1));
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn a_shred_with_a_bogus_slot_does_not_stall_the_stream() {
        let entries = entries(10);
        let (data, _) = shred(&entries, 0, true);
        let mut payload = data[0].payload().to_vec();
        payload[65..73].copy_from_slice(&(u64::MAX / 2).to_le_bytes());
        let bogus = Shred::new_from_serialized_shred(payload).unwrap();

        let mut deshredder = Deshredder::new(5);
        let completed = insert_all(
            &mut deshredder,
            std::iter::once(bogus.clone()).chain(data.clone()),
        );
        assert_eq!(completed.len(), 1, "bogus shred first");
        assert_eq!(completed[0].entries, entries);

        let mut deshredder = Deshredder::new(5);
        warm_up(&mut deshredder, &data[0]);
        let completed = insert_all(&mut deshredder, std::iter::once(bogus).chain(data));
        assert_eq!(completed.len(), 1, "bogus shred after warm-up");
        assert_eq!(deshredder.max_slot, Some(SLOT));
    }

    #[test]
    fn a_bogus_shred_during_warm_up_does_not_pick_the_slot() {
        let (data, _) = shred(&entries(10), 0, false);
        let mut payload = data[0].payload().to_vec();
        payload[65..73].copy_from_slice(&(u64::MAX / 2).to_le_bytes());
        let bogus = Shred::new_from_serialized_shred(payload).unwrap();
        let mut deshredder = Deshredder::new(5);

        deshredder.insert_shred(bogus);
        warm_up(&mut deshredder, &data[0]);
        assert_eq!(deshredder.max_slot, Some(SLOT));
        assert_eq!(deshredder.slots.keys().collect::<Vec<_>>(), [&SLOT]);
    }

    #[test]
    fn resyncs_when_the_stream_really_jumps() {
        let (old, _) = shred(&entries(10), 0, false);
        let (new, _) = shred_in_slot(SLOT + 1_000, &entries(10), 0, false);
        let mut deshredder = Deshredder::new(5);
        warm_up(&mut deshredder, &old[0]);

        for _ in 1..RESYNC_AFTER_REJECTED {
            deshredder.insert_shred(new[0].clone());
        }
        assert_eq!(deshredder.slots.keys().collect::<Vec<_>>(), [&SLOT]);

        warm_up(&mut deshredder, &new[0]);
        assert_eq!(deshredder.max_slot, Some(SLOT + 1_000));
        assert_eq!(
            deshredder.slots.keys().collect::<Vec<_>>(),
            [&(SLOT + 1_000)]
        );
    }
}
