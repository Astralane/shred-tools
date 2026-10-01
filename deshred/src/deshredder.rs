use std::{
    ops::RangeInclusive,
    time::{Duration, Instant},
};

use bytes::Bytes;
use solana_entry::entry::{Entry, MaxDataShredsLen};
use solana_ledger::shred::{Error as ShredError, ReedSolomonCache, Shred, Shredder, recover};
use solana_transaction::versioned::VersionedTransaction;
use wincode::{Deserialize, containers::Vec as WincodeVec};

const MAX_RECOVERY_ATTEMPTS: u8 = 3;
const SIZE_OF_COMMON_SHRED_HEADER: usize = 83;
const SLOT_HISTORY_SIZE: u64 = 256;

#[derive(Debug, Clone)]
pub struct CompletedDataSet {
    pub slot: u64,
    pub start_shred_index: u32,
    pub end_shred_index: u32,
    pub entries: Vec<Entry>,
}

impl CompletedDataSet {
    pub fn transactions(&self) -> impl Iterator<Item = &VersionedTransaction> {
        self.entries
            .iter()
            .flat_map(|entry| entry.transactions.iter())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DeshredStats {
    pub recoveries: u64,
    pub recovered_shreds: u64,
    pub recovery_time: Duration,
    pub decode_errors: u64,
}

pub struct Deshredder {
    reed_solomon_cache: ReedSolomonCache,
    slots: Box<[Option<SlotState>]>,
    stats: DeshredStats,
}

impl Default for Deshredder {
    fn default() -> Self {
        Self {
            reed_solomon_cache: ReedSolomonCache::default(),
            slots: (0..SLOT_HISTORY_SIZE).map(|_| None).collect(),
            stats: DeshredStats::default(),
        }
    }
}

impl Deshredder {
    pub fn insert_bytes(&mut self, payload: Bytes) -> Result<Vec<CompletedDataSet>, ShredError> {
        let shred = Shred::new_from_serialized_shred(payload)?;
        Ok(self.insert_shred(shred))
    }

    fn insert_shred(&mut self, shred: Shred) -> Vec<CompletedDataSet> {
        let Some(state) = slot_state(&mut self.slots, shred.slot()) else {
            return Vec::new();
        };
        if state.finished {
            return Vec::new();
        }

        let shred_index = shred.index();
        let fec_set_index = shred.fec_set_index();
        let mut inserted_data_range = None;
        if shred.is_data() {
            if state.insert_data_shred(shred) {
                inserted_data_range = Some(shred_index..=shred_index);
            }
        } else {
            state.insert_coding_shred(shred);
        }

        if let Some(recovery) = state.recover(fec_set_index, &self.reed_solomon_cache) {
            self.stats.recoveries += 1;
            self.stats.recovered_shreds += recovery.recovered_shreds as u64;
            self.stats.recovery_time += recovery.elapsed;
            if recovery.recovered_shreds > 0 {
                inserted_data_range = Some(recovery.fec_set_data_range);
            }
        }

        let Some(inserted_data_range) = inserted_data_range else {
            return Vec::new();
        };
        let mut data_sets = Vec::new();
        for data_set in state.take_completed_data_sets(inserted_data_range) {
            match data_set {
                Some(data_set) => data_sets.push(data_set),
                None => self.stats.decode_errors += 1,
            }
        }
        data_sets
    }

    pub fn stats(&self) -> &DeshredStats {
        &self.stats
    }
}

fn slot_state(slots: &mut [Option<SlotState>], slot: u64) -> Option<&mut SlotState> {
    let entry = &mut slots[(slot % SLOT_HISTORY_SIZE) as usize];
    match entry {
        Some(state) if state.slot > slot => return None,
        Some(state) if state.slot == slot => {}
        _ => *entry = Some(SlotState::new(slot)),
    }
    entry.as_mut()
}

struct CodingSet {
    fec_set_index: u32,
    num_data_shreds: u32,
    coding_shreds: Vec<Shred>,
    recovery_attempts: u8,
}

impl CodingSet {
    fn data_range(&self) -> RangeInclusive<u32> {
        self.fec_set_index..=self.fec_set_index + self.num_data_shreds - 1
    }
}

struct RecoveryStats {
    fec_set_data_range: RangeInclusive<u32>,
    elapsed: Duration,
    recovered_shreds: usize,
}

enum DataShred {
    Missing,
    Held(Shred),
    Emitted,
}

struct SlotState {
    slot: u64,
    data_shreds: Vec<DataShred>,
    data_set_ends: Vec<u64>,
    coding_sets: Vec<CodingSet>,
    last_shred_index: Option<u32>,
    emitted_shreds: u32,
    finished: bool,
}

impl SlotState {
    fn new(slot: u64) -> Self {
        Self {
            slot,
            data_shreds: Vec::new(),
            data_set_ends: Vec::new(),
            coding_sets: Vec::new(),
            last_shred_index: None,
            emitted_shreds: 0,
            finished: false,
        }
    }

    fn is_held(&self, shred_index: u32) -> bool {
        matches!(
            self.data_shreds.get(shred_index as usize),
            Some(DataShred::Held(_))
        )
    }

    fn insert_data_shred(&mut self, shred: Shred) -> bool {
        let shred_index = shred.index() as usize;
        if shred_index >= self.data_shreds.len() {
            self.data_shreds
                .resize_with(shred_index + 1, || DataShred::Missing);
        }
        if !matches!(self.data_shreds[shred_index], DataShred::Missing) {
            return false;
        }
        if shred.data_complete() {
            set_bit(&mut self.data_set_ends, shred_index);
        }
        if shred.last_in_slot() {
            self.last_shred_index = Some(shred_index as u32);
        }
        self.data_shreds[shred_index] = DataShred::Held(shred);
        true
    }

    fn insert_coding_shred(&mut self, shred: Shred) {
        let fec_set_index = shred.fec_set_index();
        let Some(num_data_shreds) = num_data_shreds(shred.payload().as_ref()) else {
            return;
        };
        let data_range = fec_set_index..=fec_set_index + num_data_shreds - 1;
        if missing_data_shreds(&self.data_shreds, data_range) == 0 {
            return;
        }
        let position = match self
            .coding_sets
            .iter()
            .position(|coding_set| coding_set.fec_set_index == fec_set_index)
        {
            Some(position) => position,
            None => {
                self.coding_sets.push(CodingSet {
                    fec_set_index,
                    num_data_shreds,
                    coding_shreds: Vec::new(),
                    recovery_attempts: 0,
                });
                self.coding_sets.len() - 1
            }
        };
        let coding_shreds = &mut self.coding_sets[position].coding_shreds;
        if !coding_shreds
            .iter()
            .any(|held| held.index() == shred.index())
        {
            coding_shreds.push(shred);
        }
    }

    fn recover(&mut self, fec_set_index: u32, cache: &ReedSolomonCache) -> Option<RecoveryStats> {
        let position = self
            .coding_sets
            .iter()
            .position(|coding_set| coding_set.fec_set_index == fec_set_index)?;
        let coding_set = &self.coding_sets[position];
        let data_range = coding_set.data_range();
        if missing_data_shreds(&self.data_shreds, data_range.clone()) == 0 {
            self.coding_sets.swap_remove(position);
            return None;
        }
        let held = data_range
            .clone()
            .filter(|&index| self.is_held(index))
            .count();
        if held + coding_set.coding_shreds.len() < coding_set.num_data_shreds as usize
            || coding_set.recovery_attempts >= MAX_RECOVERY_ATTEMPTS
        {
            return None;
        }

        let started = Instant::now();
        let recovery_input: Vec<Shred> = data_range
            .clone()
            .filter_map(|index| match self.data_shreds.get(index as usize) {
                Some(DataShred::Held(shred)) => Some(shred.clone()),
                _ => None,
            })
            .chain(coding_set.coding_shreds.iter().cloned())
            .collect();
        self.coding_sets[position].recovery_attempts += 1;
        let recovered_shreds = recover(recovery_input, cache)
            .ok()?
            .flatten()
            .filter(|shred| shred.is_data() && self.insert_data_shred(shred.clone()))
            .count();
        if missing_data_shreds(&self.data_shreds, data_range.clone()) == 0 {
            self.coding_sets.swap_remove(position);
        }
        Some(RecoveryStats {
            fec_set_data_range: data_range,
            elapsed: started.elapsed(),
            recovered_shreds,
        })
    }

    fn take_completed_data_sets(
        &mut self,
        inserted_data_range: RangeInclusive<u32>,
    ) -> Vec<Option<CompletedDataSet>> {
        let (inserted_start, inserted_end) = inserted_data_range.into_inner();
        let mut data_set_start =
            last_bit_before(&self.data_set_ends, inserted_start).map_or(0, |end| end + 1);
        let mut search_from = inserted_start;
        let mut data_sets = Vec::new();
        while let Some(data_set_end) = next_bit_from(&self.data_set_ends, search_from) {
            if (data_set_start..=data_set_end).all(|index| self.is_held(index)) {
                data_sets.push(self.take_data_set(data_set_start..=data_set_end));
            }
            if data_set_end > inserted_end {
                break;
            }
            data_set_start = data_set_end + 1;
            search_from = data_set_start;
        }
        data_sets
    }

    fn take_data_set(&mut self, range: RangeInclusive<u32>) -> Option<CompletedDataSet> {
        let (start, end) = (*range.start(), *range.end());
        let mut data_shreds = Vec::with_capacity((end - start + 1) as usize);
        for entry in &mut self.data_shreds[start as usize..=end as usize] {
            if let DataShred::Held(shred) = std::mem::replace(entry, DataShred::Emitted) {
                data_shreds.push(shred);
            }
        }
        self.emitted_shreds += end - start + 1;
        let emitted = &self.data_shreds;
        self.coding_sets.retain(|coding_set| {
            let data_range = coding_set.data_range();
            let overlaps = *data_range.start() <= end && *data_range.end() >= start;
            !overlaps || missing_data_shreds(emitted, data_range) > 0
        });
        if self
            .last_shred_index
            .is_some_and(|last| self.emitted_shreds == last + 1)
        {
            self.finish();
        }

        let bytes = Shredder::deshred(data_shreds.iter().map(Shred::payload)).ok()?;
        let entries =
            <WincodeVec<Entry, MaxDataShredsLen> as Deserialize>::deserialize(&bytes).ok()?;
        Some(CompletedDataSet {
            slot: self.slot,
            start_shred_index: start,
            end_shred_index: end,
            entries,
        })
    }

    fn finish(&mut self) {
        self.finished = true;
        self.data_shreds = Vec::new();
        self.data_set_ends = Vec::new();
        self.coding_sets = Vec::new();
    }
}

fn missing_data_shreds(data_shreds: &[DataShred], data_range: RangeInclusive<u32>) -> usize {
    data_range
        .filter(|&index| {
            matches!(
                data_shreds.get(index as usize),
                None | Some(DataShred::Missing)
            )
        })
        .count()
}

fn set_bit(bits: &mut Vec<u64>, index: usize) {
    let word = index / 64;
    if word >= bits.len() {
        bits.resize(word + 1, 0);
    }
    bits[word] |= 1 << (index % 64);
}

fn last_bit_before(bits: &[u64], index: u32) -> Option<u32> {
    let index = index as usize;
    let (mut word, mut mask) = match bits.get(index / 64) {
        Some(bits) => (index / 64, bits & ((1u64 << (index % 64)) - 1)),
        None => (bits.len(), 0),
    };
    loop {
        if mask != 0 {
            return Some((word * 64 + 63 - mask.leading_zeros() as usize) as u32);
        }
        word = word.checked_sub(1)?;
        mask = bits[word];
    }
}

fn next_bit_from(bits: &[u64], index: u32) -> Option<u32> {
    let index = index as usize;
    let mut word = index / 64;
    let mut mask = bits.get(word)? & (u64::MAX << (index % 64));
    loop {
        if mask != 0 {
            return Some((word * 64 + mask.trailing_zeros() as usize) as u32);
        }
        word += 1;
        mask = *bits.get(word)?;
    }
}

fn num_data_shreds(coding_shred_payload: &[u8]) -> Option<u32> {
    let bytes =
        coding_shred_payload.get(SIZE_OF_COMMON_SHRED_HEADER..SIZE_OF_COMMON_SHRED_HEADER + 2)?;
    let num_data_shreds = u16::from_le_bytes(bytes.try_into().ok()?);
    (num_data_shreds > 0).then_some(u32::from(num_data_shreds))
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

    fn insert_all(
        deshredder: &mut Deshredder,
        shreds: impl IntoIterator<Item = Shred>,
    ) -> Vec<CompletedDataSet> {
        shreds
            .into_iter()
            .flat_map(|shred| deshredder.insert_shred(shred))
            .collect()
    }

    #[test]
    fn coding_shreds_report_the_fec_sets_data_count() {
        let (_, coding) = shred(&entries(1_000), 0, false);
        assert_eq!(
            num_data_shreds(coding[0].payload().as_ref()),
            Some(DATA_SHREDS_PER_FEC_BLOCK as u32)
        );
    }

    #[test]
    fn decodes_a_complete_data_set() {
        let entries = entries(1_000);
        let (data, _) = shred(&entries, 0, false);
        let last = data.last().unwrap().index();

        let completed = insert_all(&mut Deshredder::default(), data);

        assert_eq!(completed.len(), 1);
        assert_eq!(
            (
                completed[0].slot,
                completed[0].start_shred_index,
                completed[0].end_shred_index
            ),
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
        let mut deshredder = Deshredder::default();

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
        let mut deshredder = Deshredder::default();
        insert_all(&mut deshredder, data.into_iter().skip(1));

        insert_all(
            &mut deshredder,
            coding.into_iter().filter(|s| s.fec_set_index() == fec),
        );
        let stats = deshredder.stats();
        assert_eq!((stats.recoveries, stats.recovered_shreds), (1, 1));
    }

    #[test]
    fn recovery_only_counts_data_shreds_still_held() {
        let (data, coding) = shred(&entries(1_000), 0, false);
        let fec = data[0].fec_set_index();
        let num_data = DATA_SHREDS_PER_FEC_BLOCK as u32;
        let missing = fec + num_data - 1;
        let mut state = SlotState::new(SLOT);
        state
            .data_shreds
            .resize_with((fec + num_data / 2) as usize, || DataShred::Emitted);
        for shred in data.into_iter().filter(|s| s.index() != missing) {
            state.insert_data_shred(shred);
        }
        let mut coding = coding.into_iter().filter(|s| s.fec_set_index() == fec);
        let cache = ReedSolomonCache::default();

        state.insert_coding_shred(coding.next().unwrap());
        assert!(state.recover(fec, &cache).is_none());
        assert_eq!(state.coding_sets[0].recovery_attempts, 0);

        let held = (fec..fec + num_data).filter(|&i| state.is_held(i)).count() + 1;
        for shred in coding.take(num_data as usize - held) {
            state.insert_coding_shred(shred);
        }
        let recovery = state.recover(fec, &cache).unwrap();
        assert_eq!(recovery.recovered_shreds, 1);
        assert!(state.is_held(missing));
        assert!(state.coding_sets.is_empty());
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

        let mut deshredder = Deshredder::default();
        let completed = insert_all(&mut deshredder, first.into_iter().chain(second));

        assert_eq!(deshredder.stats().decode_errors, 1);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start_shred_index, next);
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn a_slot_is_replaced_by_the_one_a_full_history_later() {
        let (old, _) = shred(&entries(10), 0, false);
        let (new, _) = shred_in_slot(SLOT + SLOT_HISTORY_SIZE, &entries(10), 0, false);
        let mut deshredder = Deshredder::default();
        let held = |deshredder: &Deshredder| {
            deshredder
                .slots
                .iter()
                .flatten()
                .map(|state| state.slot)
                .collect::<Vec<_>>()
        };

        deshredder.insert_shred(old[0].clone());
        assert_eq!(held(&deshredder), [SLOT]);

        deshredder.insert_shred(new[0].clone());
        assert_eq!(held(&deshredder), [SLOT + SLOT_HISTORY_SIZE]);

        deshredder.insert_shred(old[1].clone());
        assert_eq!(held(&deshredder), [SLOT + SLOT_HISTORY_SIZE]);
    }

    #[test]
    fn a_later_data_set_does_not_wait_for_an_earlier_one() {
        let entries = entries(10);
        let (first, _) = shred(&entries, 0, false);
        let next = first.last().unwrap().index() + 1;
        let (second, _) = shred(&entries, next, true);
        let lost = first[0].clone();
        let mut deshredder = Deshredder::default();

        let completed = insert_all(&mut deshredder, first.into_iter().skip(1).chain(second));
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start_shred_index, next);

        let completed = insert_all(&mut deshredder, [lost.clone(), lost]);
        assert_eq!(completed.len(), 1);
        assert_eq!(
            (completed[0].start_shred_index, completed[0].end_shred_index),
            (0, next - 1)
        );
        assert_eq!(completed[0].entries, entries);
    }

    #[test]
    fn a_finished_slot_frees_its_shreds_and_ignores_late_ones() {
        let entries = entries(1_000);
        let (data, coding) = shred(&entries, 0, true);
        let mut deshredder = Deshredder::default();

        assert_eq!(insert_all(&mut deshredder, data.clone()).len(), 1);
        let state = deshredder.slots.iter().flatten().next().unwrap();
        assert!(state.finished && state.data_shreds.is_empty());

        assert!(insert_all(&mut deshredder, coding.into_iter().chain(data)).is_empty());
        assert_eq!(deshredder.stats().recoveries, 0);
    }

    #[test]
    fn finds_data_set_ends_across_words() {
        let mut bits = Vec::new();
        for index in [3, 64, 200] {
            set_bit(&mut bits, index);
        }
        assert_eq!(last_bit_before(&bits, 3), None);
        assert_eq!(last_bit_before(&bits, 64), Some(3));
        assert_eq!(last_bit_before(&bits, 1_000), Some(200));
        assert_eq!(next_bit_from(&bits, 4), Some(64));
        assert_eq!(next_bit_from(&bits, 65), Some(200));
        assert_eq!(next_bit_from(&bits, 201), None);
    }
}
