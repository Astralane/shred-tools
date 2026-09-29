//! FEC-set aggregation per (provider, slot, fec_set_index). All timestamps are
//! kernel CLOCK_REALTIME stamps from one machine, so provider deltas are exact.

use ahash::{AHashMap, AHashSet};
use solana_sdk::pubkey::Pubkey;

use crate::{registry::ProviderId, verify::VerifiedShred};

const MAX_POSITION: u32 = 128;

#[derive(Clone)]
pub struct SetRow {
    pub provider: ProviderId,
    pub slot: u64,
    pub fec_set_index: u32,
    pub leader: Option<Pubkey>,
    pub first_ns: i64,
    /// Arrival of the k-th distinct shred (k = num_data): when the set became decodable.
    pub decode_ns: Option<i64>,
    pub last_ns: i64,
    pub n_data: u32,
    pub n_code: u32,
    pub expected_total: Option<u32>,
    pub missed: u32,
    /// `invalid_sig + invalid_data + invalid_unknown`.
    pub invalid: u32,
    /// Failed verification, but the block data matches a leader-signed copy.
    pub invalid_sig: u32,
    /// Failed verification and the block data differs from a leader-signed copy.
    pub invalid_data: u32,
    /// Failed verification with no leader-signed copy to compare against.
    pub invalid_unknown: u32,
    pub duplicated: u32,
    pub sig_unverifiable: u32,
    pub is_valid: bool,
    pub last_in_slot: bool,
}

struct SetState {
    leader: Option<Pubkey>,
    seen: AHashSet<u64>,
    data_pos: u128,
    code_pos: u128,
    num_data: Option<u16>,
    num_coding: Option<u16>,
    /// Fallback timestamp for rows where no shred was accepted.
    created_ns: i64,
    /// Accepted shreds only, so an invalid shred can't make a provider look faster.
    first_ns: Option<i64>,
    last_ns: Option<i64>,
    /// One per new position; `num_data` may be learned late, so the k-th is picked at finalize.
    arrivals: Vec<i64>,
    /// `(is_code, shred_index, data_hash)`; classified at finalize since the
    /// leader-signed copy may arrive later from another provider.
    invalid_shreds: Vec<(bool, u32, Option<[u8; 32]>)>,
    duplicated: u32,
    unverifiable: u32,
    last_in_slot: bool,
}

impl SetState {
    fn new(leader: Option<Pubkey>, ns: i64) -> Self {
        Self {
            leader,
            seen: AHashSet::new(),
            data_pos: 0,
            code_pos: 0,
            num_data: None,
            num_coding: None,
            created_ns: ns,
            first_ns: None,
            last_ns: None,
            arrivals: Vec::new(),
            invalid_shreds: Vec::new(),
            duplicated: 0,
            unverifiable: 0,
            last_in_slot: false,
        }
    }
}

pub struct Aggregator {
    sets: AHashMap<(ProviderId, u64, u32), SetState>,
    /// `(slot, fec_set_index, is_code, shred_index) -> data hash` from shreds whose
    /// leader signature verified. `is_code` must stay in the key: data and coding
    /// index spaces overlap, and mixing them misreports broken proofs as altered data.
    truth: AHashMap<(u64, u32, bool, u32), [u8; 32]>,
    max_slot: u64,
    max_wait_slots: u64,
    /// Highest harvested cutoff; sets at or below it are already emitted.
    evicted_upto: u64,
    shreds_after_window: u64,
}

impl Aggregator {
    pub fn new(max_wait_slots: u64) -> Self {
        Self {
            sets: AHashMap::new(),
            truth: AHashMap::new(),
            max_slot: 0,
            max_wait_slots,
            evicted_upto: 0,
            shreds_after_window: 0,
        }
    }

    pub fn pending_sets(&self) -> usize {
        self.sets.len()
    }

    pub fn max_slot(&self) -> u64 {
        self.max_slot
    }

    pub fn shreds_after_window(&self) -> u64 {
        self.shreds_after_window
    }

    pub fn ingest(&mut self, s: &VerifiedShred) {
        self.max_slot = self.max_slot.max(s.slot);
        // Re-creating an emitted set would produce a second row that overwrites the
        // real one (viewer is last-wins). Sets drained early by rotation can still be
        // re-created — a known limitation.
        if s.slot <= self.evicted_upto {
            self.shreds_after_window += 1;
            return;
        }
        let st = self
            .sets
            .entry((s.provider, s.slot, s.fec_set_index))
            .or_insert_with(|| SetState::new(s.leader, s.rx_unix_ns));

        if !st.seen.insert(s.payload_hash) {
            st.duplicated += 1;
            return;
        }

        match (s.merkle_ok, s.sig_ok) {
            (false, _) | (_, Some(false)) => {
                st.invalid_shreds.push((s.is_code, s.shred_index, s.data_hash));
                return;
            }
            (true, None) => {
                st.unverifiable += 1;
                return;
            }
            (true, Some(true)) => {}
        }

        if let Some(h) = s.data_hash {
            self.truth.insert((s.slot, s.fec_set_index, s.is_code, s.shred_index), h);
        }

        let is_new_position = if s.is_code {
            if st.num_data.is_none() {
                st.num_data = s.num_data;
                st.num_coding = s.num_coding;
            }
            mark_position(&mut st.code_pos, s.position)
        } else {
            st.last_in_slot |= s.last_in_slot;
            // DATA_COMPLETE marks the set's last data shred, so data-only providers
            // can learn num_data without any coding shred.
            if st.num_data.is_none() && s.data_complete {
                st.num_data = Some((s.position + 1) as u16);
            }
            mark_position(&mut st.data_pos, s.position)
        };
        if is_new_position {
            st.arrivals.push(s.rx_unix_ns);
        }

        st.first_ns = Some(st.first_ns.map_or(s.rx_unix_ns, |f| f.min(s.rx_unix_ns)));
        st.last_ns = Some(st.last_ns.map_or(s.rx_unix_ns, |l| l.max(s.rx_unix_ns)));
    }

    /// Emit every set far enough behind the tip; `drain_all` emits everything.
    pub fn harvest(&mut self, drain_all: bool) -> Vec<SetRow> {
        let cutoff = self.max_slot.saturating_sub(self.max_wait_slots);
        let ready: Vec<(ProviderId, u64, u32)> = self
            .sets
            .keys()
            .filter(|(_, slot, _)| drain_all || *slot <= cutoff)
            .copied()
            .collect();
        let out = ready
            .into_iter()
            .map(|key| {
                let st = self.sets.remove(&key).unwrap();
                finalize(key, st, &self.truth)
            })
            .collect();

        // Only prune `truth` when the cutoff advances; harvest runs every batch.
        if drain_all {
            self.truth.clear();
            self.evicted_upto = cutoff;
        } else if cutoff > self.evicted_upto {
            self.truth.retain(|(slot, _, _, _), _| *slot > cutoff);
            self.evicted_upto = cutoff;
        }
        out
    }
}

/// Sets bit `pos` and returns whether it was new. `1u128 << 128` overflows, hence the guard.
fn mark_position(bits: &mut u128, pos: u32) -> bool {
    if pos >= MAX_POSITION {
        return false;
    }
    let bit = 1u128 << pos;
    let new = *bits & bit == 0;
    *bits |= bit;
    new
}

fn finalize(
    (provider, slot, fec_set_index): (ProviderId, u64, u32),
    mut st: SetState,
    truth: &AHashMap<(u64, u32, bool, u32), [u8; 32]>,
) -> SetRow {
    let (mut invalid_sig, mut invalid_data, mut invalid_unknown) = (0u32, 0u32, 0u32);
    for (is_code, shred_index, data) in &st.invalid_shreds {
        match (data, truth.get(&(slot, fec_set_index, *is_code, *shred_index))) {
            (Some(got), Some(want)) if got == want => invalid_sig += 1,
            (Some(_), Some(_)) => invalid_data += 1,
            _ => invalid_unknown += 1,
        }
    }
    let invalid = st.invalid_shreds.len() as u32;

    let n_data = st.data_pos.count_ones();
    let n_code = st.code_pos.count_ones();
    let expected_total = st
        .num_data
        .zip(st.num_coding)
        .map(|(d, c)| d as u32 + c as u32);
    let delivered = n_data + n_code;
    let missed = expected_total.unwrap_or(delivered).saturating_sub(delivered);

    // Reed-Solomon: any k of the k+m shreds reconstruct the set.
    let k = st.num_data.unwrap_or(0) as usize;
    let decode_ns = if k > 0 && st.arrivals.len() >= k {
        Some(*st.arrivals.select_nth_unstable(k - 1).1)
    } else {
        None
    };

    let is_valid = invalid == 0 && st.unverifiable == 0 && decode_ns.is_some();

    SetRow {
        provider,
        slot,
        fec_set_index,
        leader: st.leader,
        first_ns: st.first_ns.unwrap_or(st.created_ns),
        decode_ns,
        last_ns: st.last_ns.unwrap_or(st.created_ns),
        n_data,
        n_code,
        expected_total,
        missed,
        invalid,
        invalid_sig,
        invalid_data,
        invalid_unknown,
        duplicated: st.duplicated,
        sig_unverifiable: st.unverifiable,
        is_valid,
        last_in_slot: st.last_in_slot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shred(provider: ProviderId, slot: u64, fec: u32, pos: u32, rx: i64, hash: u64) -> VerifiedShred {
        VerifiedShred {
            provider,
            rx_unix_ns: rx,
            slot,
            fec_set_index: fec,
            shred_index: fec + pos,
            is_code: false,
            position: pos,
            last_in_slot: false,
            data_complete: false,
            num_data: None,
            num_coding: None,
            leader: None,
            sig_ok: Some(true),
            merkle_ok: true,
            payload_hash: hash,
            data_hash: Some(leaf(hash)),
        }
    }

    fn coding(provider: ProviderId, slot: u64, fec: u32, pos: u32, rx: i64, hash: u64, nd: u16) -> VerifiedShred {
        VerifiedShred {
            is_code: true,
            num_data: Some(nd),
            num_coding: Some(nd),
            ..shred(provider, slot, fec, pos, rx, hash)
        }
    }

    /// Equal `hash` => equal block data.
    fn leaf(hash: u64) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&hash.to_le_bytes());
        h
    }

    fn bad(
        provider: ProviderId,
        slot: u64,
        fec: u32,
        pos: u32,
        rx: i64,
        data_hash: Option<[u8; 32]>,
    ) -> VerifiedShred {
        let mut s = shred(provider, slot, fec, pos, rx, 0xdead_0000 + pos as u64);
        s.sig_ok = Some(false);
        s.data_hash = data_hash;
        s
    }

    fn with_leader(mut s: VerifiedShred) -> VerifiedShred {
        s.leader = Some(Pubkey::new_unique());
        s
    }

    #[test]
    fn broken_proof_over_genuine_data_is_invalid_sig_not_invalid_data() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_000, 0xAAAA)));
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xAAAA)))));

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid, 1);
        assert_eq!(r.invalid_sig, 1, "data matched the leader-signed copy");
        assert_eq!(r.invalid_data, 0, "must not be reported as altered content");
        assert_eq!(r.invalid_unknown, 0);
        assert!(!r.is_valid, "it still failed verification and is still not valid");
    }

    #[test]
    fn altered_block_data_is_invalid_data() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_000, 0xAAAA)));
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xBBBB)))));

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid_data, 1, "leaf differs from the leader-signed copy");
        assert_eq!(r.invalid_sig, 0);
        assert_eq!(r.invalid_unknown, 0);
    }

    #[test]
    fn without_an_authenticated_copy_the_split_is_unknown() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xBBBB)))));

        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.invalid, 1);
        assert_eq!(r.invalid_unknown, 1);
        assert_eq!(r.invalid_sig, 0);
        assert_eq!(r.invalid_data, 0);
    }

    #[test]
    fn truth_arriving_after_the_bad_copy_still_classifies_it() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xAAAA)))));
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_200, 0xAAAA)));

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid_sig, 1, "late ground truth must still be applied");
        assert_eq!(r.invalid_unknown, 0);
    }

    /// Regression: a truth key without `is_code` reported 15,456 false "bad data" shreds in production.
    #[test]
    fn a_coding_shred_is_not_ground_truth_for_a_data_shred_at_the_same_index() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_000, 0xAAAA)));
        agg.ingest(&with_leader(coding(0, 100, 0, 3, 1_010, 0xCCCC, 32)));
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xAAAA)))));

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid_data, 0, "a coding shred's hash must never be the yardstick for a data shred");
        assert_eq!(r.invalid_sig, 1, "the block data matched the leader-signed data shred");
    }

    #[test]
    fn a_data_only_provider_can_still_decode_a_set() {
        let mut agg = Aggregator::new(0);
        for pos in 0..4u32 {
            let mut s = with_leader(shred(0, 100, 0, pos, 1_000 + pos as i64, 0xA000 + pos as u64));
            s.data_complete = pos == 3;
            agg.ingest(&s);
        }

        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.n_data, 4);
        assert_eq!(r.n_code, 0);
        assert!(r.decode_ns.is_some(), "a data-only provider that delivered every data shred must decode");
        assert!(r.is_valid);
    }

    #[test]
    fn split_sums_to_invalid() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(shred(0, 100, 0, 1, 1_000, 0x1111)));
        agg.ingest(&with_leader(shred(0, 100, 0, 2, 1_000, 0x2222)));
        agg.ingest(&with_leader(bad(1, 100, 0, 1, 1_100, Some(leaf(0x1111))))); // sig
        agg.ingest(&with_leader(bad(1, 100, 0, 2, 1_100, Some(leaf(0x9999))))); // data
        agg.ingest(&with_leader(bad(1, 100, 0, 7, 1_100, Some(leaf(0x3333))))); // unknown

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid, 3);
        assert_eq!(r.invalid_sig + r.invalid_data + r.invalid_unknown, r.invalid);
        assert_eq!((r.invalid_sig, r.invalid_data, r.invalid_unknown), (1, 1, 1));
    }

    #[test]
    fn invalid_first_shred_does_not_move_first_ns() {
        let mut agg = Aggregator::new(10);
        let mut bad = with_leader(shred(0, 5, 0, 0, 100, 0xAA));
        bad.sig_ok = Some(false);
        agg.ingest(&bad);
        agg.ingest(&with_leader(shred(0, 5, 0, 1, 200, 0xBB)));

        let rows = agg.harvest(true);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.first_ns, 200, "invalid shred at t=100 poisoned first_ns");
        assert_eq!(r.invalid, 1);
    }

    #[test]
    fn decode_ns_is_the_kth_arrival_not_when_num_data_was_learned() {
        let mut agg = Aggregator::new(10);
        agg.ingest(&with_leader(shred(0, 7, 0, 0, 1000, 1)));
        agg.ingest(&with_leader(coding(0, 7, 0, 0, 1100, 2, 1)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert!(r.is_valid, "set with enough data shreds should be valid");
        assert_eq!(r.decode_ns, Some(1000));
        assert_eq!(r.first_ns, 1000);
        assert_eq!(r.last_ns, 1100);
    }

    #[test]
    fn decode_ns_when_num_data_is_learned_only_from_a_late_coding_shred() {
        let mut agg = Aggregator::new(10);
        for pos in 0..3u32 {
            agg.ingest(&with_leader(shred(0, 8, 0, pos, 1000 + pos as i64, 0xD000 + pos as u64)));
        }
        agg.ingest(&with_leader(coding(0, 8, 0, 0, 5000, 0xC0DE, 3)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert!(r.is_valid);
        assert_eq!(r.decode_ns, Some(1002), "decodable at the 3rd data shred, not when k was revealed");
    }

    #[test]
    fn duplicate_does_not_extend_last_ns() {
        let mut agg = Aggregator::new(10);
        agg.ingest(&with_leader(shred(0, 3, 0, 0, 500, 0x11)));
        agg.ingest(&with_leader(shred(0, 3, 0, 0, 9999, 0x11)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.duplicated, 1);
        assert_eq!(r.last_ns, 500, "duplicate retransmit must not extend last_ns");
    }

    #[test]
    fn no_leader_is_unverifiable_not_invalid() {
        let mut agg = Aggregator::new(10);
        let mut s = shred(0, 9, 0, 0, 100, 0x22);
        s.sig_ok = None;
        agg.ingest(&s);
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.sig_unverifiable, 1);
        assert_eq!(r.invalid, 0);
        assert!(!r.is_valid);
    }

    #[test]
    fn harvest_respects_max_wait_slots() {
        let mut agg = Aggregator::new(10);
        agg.ingest(&with_leader(shred(0, 100, 0, 0, 1, 0x1)));
        assert!(agg.harvest(false).is_empty());
        agg.ingest(&with_leader(shred(0, 200, 0, 0, 2, 0x2)));
        let rows = agg.harvest(false);
        assert!(rows.iter().any(|r| r.slot == 100), "aged set should be harvested");
    }

    #[test]
    fn separate_providers_do_not_share_a_set() {
        let mut agg = Aggregator::new(10);
        agg.ingest(&with_leader(shred(0, 5, 0, 0, 100, 0xA)));
        agg.ingest(&with_leader(shred(1, 5, 0, 0, 150, 0xA)));
        let rows = agg.harvest(true);
        assert_eq!(rows.len(), 2, "each provider must get its own row for the same set");
    }

    #[test]
    fn straggler_after_window_is_dropped_and_counted() {
        let mut agg = Aggregator::new(10);
        let mut good = with_leader(shred(0, 100, 0, 0, 1_000, 0xAAAA));
        good.data_complete = true;
        agg.ingest(&good);

        agg.ingest(&with_leader(shred(0, 200, 0, 0, 2_000, 0xBBBB)));
        let rows = agg.harvest(false);
        let r = rows.iter().find(|r| r.slot == 100).expect("slot 100 finalized");
        assert!(r.is_valid);
        assert_eq!(r.decode_ns, Some(1_000));

        agg.ingest(&with_leader(shred(1, 100, 0, 5, 3_000, 0xDEAD)));
        assert_eq!(agg.shreds_after_window(), 1, "the late shred must be counted");

        let rows2 = agg.harvest(true);
        assert!(
            rows2.iter().all(|r| r.slot != 100),
            "a finalized set must never be resurrected by a straggler"
        );
    }
}
