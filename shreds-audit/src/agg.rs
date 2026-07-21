//! FEC-set aggregation, per (provider, slot, fec_set_index).
//!
//! Every timestamp is an absolute CLOCK_REALTIME nanosecond
//! value from the kernel, on one machine, from one clock. Provider-to-provider
//! deltas are therefore exact subtractions with no anchor to cancel and no
//! anchor to distort — which is the entire reason this tool exists.

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
    /// Arrival of this provider's first accepted shred of the set.
    pub first_ns: i64,
    /// Arrival of the shred that completed `num_data` — the moment the set
    /// becomes decodable. This is the edge that actually matters downstream.
    pub decode_ns: Option<i64>,
    /// Arrival of this provider's last accepted shred.
    pub last_ns: i64,
    pub n_data: u32,
    pub n_code: u32,
    pub expected_total: Option<u32>,
    pub missed: u32,
    /// Total that failed verification: `invalid_sig + invalid_data + invalid_unknown`.
    pub invalid: u32,
    /// Failed verification, but the block data is provably the leader's: the
    /// shred's leaf is byte-identical to a copy whose leader signature verified.
    /// Only the merkle proof / signature path is broken.
    pub invalid_sig: u32,
    /// Failed verification *and* the block data differs from the leader-signed
    /// copy. The provider did not merely mangle the proof — it changed content.
    pub invalid_data: u32,
    /// Failed verification, and no leader-authenticated copy of that exact shred
    /// was seen from anyone, so we cannot say which of the two it was. Never
    /// folded into either bucket.
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
    /// Arrival of the set's creating shred, valid or not. Only used as a
    /// fallback timestamp for all-invalid rows (which are `is_valid = false`
    /// and never enter a timing comparison); never mixed into `first_ns`.
    created_ns: i64,
    /// `first_ns`/`last_ns` track ACCEPTED (valid, verified) shreds only.
    /// `None` until the first accepted shred, so an invalid shred that happens
    /// to arrive first cannot pull `first_ns` earlier and make the provider
    /// look faster than it really delivered.
    first_ns: Option<i64>,
    last_ns: Option<i64>,
    /// Arrival of every accepted shred that added a NEW position — one per distinct
    /// delivered shred. `num_data` can be learned late, so the decisive k-th arrival
    /// isn't knowable at ingest; keep them all and pick the k-th at finalize.
    arrivals: Vec<i64>,
    /// Every shred of this set that failed verification, held as
    /// `(is_code, shred_index, data_hash)` until the set is finalized. `is_code`
    /// is carried because data and coding shreds share an index space and must
    /// never be compared against each other. Classification is
    /// deferred on purpose: the leader-authenticated copy we compare against may
    /// arrive from another provider *after* the bad one, so the verdict can only
    /// be settled once the set has stopped moving.
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
    /// `(slot, fec_set_index, is_code, shred_index) -> data hash`, recorded from
    /// shreds whose **leader signature verified**. That signature is what makes
    /// the entry authoritative: it proves the block data is the leader's, byte for
    /// byte. Nothing here is taken on a provider's word — a dishonest provider
    /// cannot forge an entry without forging the leader's signature.
    ///
    /// Keyed across providers on purpose: whoever delivers a shred correctly
    /// supplies the ground truth against which everyone else's copy is judged.
    ///
    /// `is_code` is part of the key and must stay there. Data and coding shreds
    /// have **separate index spaces that overlap numerically** — in real traffic
    /// almost every `(slot, fec_set_index, shred_index)` names both a data shred
    /// and a coding shred. Drop `is_code` and a data shred gets measured against a
    /// coding shred's hash, they differ, and every broken proof is reported as
    /// altered block data: a false accusation of substitution.
    truth: AHashMap<(u64, u32, bool, u32), [u8; 32]>,
    max_slot: u64,
    max_wait_slots: u64,
    /// Highest cutoff we have already evicted `truth` up to. `harvest` runs on
    /// every receive batch, and a full `retain` over the truth map each time is
    /// an O(entries) scan for nothing — the map only becomes stale when the
    /// cutoff advances, which happens once per slot, not once per batch.
    ///
    /// It doubles as the *finalized floor*: every set at or below this slot has
    /// already been emitted, so a shred arriving for one must be dropped rather
    /// than allowed to resurrect it (see `ingest`).
    evicted_upto: u64,
    /// Shreds dropped because their slot was at or below `evicted_upto` — their
    /// set was already finalized and written. Surfaced as `shreds_after_window`.
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

    pub fn shreds_after_window(&self) -> u64 {
        self.shreds_after_window
    }

    pub fn ingest(&mut self, s: &VerifiedShred) {
        // A shred at or below the finalized floor belongs to a set already emitted.
        // Re-inserting it would create a fresh SetState and a second row for the same
        // (provider, slot, fec_set_index); the viewer resolves duplicate keys
        // last-wins, so that one-shred `is_valid = false` straggler would overwrite
        // the provider's real, often winning, row. Drop and count it instead.
        // (`evicted_upto` only tracks the harvest cutoff, so sets drained early by
        // rotation (slot > cutoff) can still be re-created — a known limitation.)
        if s.slot <= self.evicted_upto {
            self.shreds_after_window += 1;
            return;
        }
        let key = (s.provider, s.slot, s.fec_set_index);
        let st = self
            .sets
            .entry(key)
            .or_insert_with(|| SetState::new(s.leader, s.rx_unix_ns));

        // Byte-identical retransmit from the same provider. Never moves a
        // timestamp: a duplicate arriving late must not extend `last_ns`.
        if !st.seen.insert(s.payload_hash) {
            st.duplicated += 1;
            return;
        }

        // A shred whose merkle proof does not reconstruct the root, or whose
        // signature fails, is counted but contributes no position and no
        // timestamp. Otherwise a provider could look fast by spraying garbage.
        match (s.merkle_ok, s.sig_ok) {
            (false, _) | (_, Some(false)) => {
                st.invalid_shreds.push((s.is_code, s.shred_index, s.data_hash));
                return;
            }
            (true, None) => {
                // No leader for this slot: we cannot judge. Count it, and keep it
                // out of the timing so an unverifiable shred can never win a race.
                st.unverifiable += 1;
                return;
            }
            (true, Some(true)) => {}
        }

        // Accepted: the leader's signature verified over this shred's merkle
        // root, so its leaf *is* the leader's block data. Record it as the truth
        // for this (slot, fec_set_index, shred_index) — it is what any failing
        // copy of the same shred, from any provider, gets measured against.
        if let Some(h) = s.data_hash {
            self.truth.insert((s.slot, s.fec_set_index, s.is_code, s.shred_index), h);
        }

        // Whether this shred adds a position not held yet. Data and coding occupy
        // separate position spaces, so a coding shred never masks a data one. The
        // shift is guarded on `< MAX_POSITION` because `1u128 << 128` is undefined.
        let is_new_position = if s.is_code {
            if st.num_data.is_none() {
                st.num_data = s.num_data;
                st.num_coding = s.num_coding;
            }
            if s.position < MAX_POSITION {
                let bit = 1u128 << s.position;
                let new = st.code_pos & bit == 0;
                st.code_pos |= bit;
                new
            } else {
                false
            }
        } else {
            if s.last_in_slot {
                st.last_in_slot = true;
            }
            // A coding shred's header is the best source of the set's data-shred
            // count, but it is not the only one: the data shred flagged
            // DATA_COMPLETE is the last of the set, so `position + 1` is the count.
            // Without this a provider that forwards only data shreds could never
            // learn `num_data`, would be `is_valid = false` on every set, and would
            // score a 0% winrate while in fact delivering everything needed.
            if st.num_data.is_none() && s.data_complete {
                st.num_data = Some((s.position + 1) as u16);
            }
            if s.position < MAX_POSITION {
                let bit = 1u128 << s.position;
                let new = st.data_pos & bit == 0;
                st.data_pos |= bit;
                new
            } else {
                false
            }
        };
        // Keep the arrival of each distinct delivered shred; `decode_ns` is the
        // k-th of these, computed at finalize once `num_data` is settled.
        if is_new_position {
            st.arrivals.push(s.rx_unix_ns);
        }

        st.first_ns = Some(st.first_ns.map_or(s.rx_unix_ns, |f| f.min(s.rx_unix_ns)));
        st.last_ns = Some(st.last_ns.map_or(s.rx_unix_ns, |l| l.max(s.rx_unix_ns)));

        if s.slot > self.max_slot {
            self.max_slot = s.slot;
        }
    }

    /// Emit every set the leader has moved far enough past. `drain_all` forces
    /// everything out, for shutdown and archive rotation.
    pub fn harvest(&mut self, drain_all: bool) -> Vec<SetRow> {
        let cutoff = self.max_slot.saturating_sub(self.max_wait_slots);
        let ready: Vec<(ProviderId, u64, u32)> = self
            .sets
            .keys()
            .filter(|(_, slot, _)| drain_all || *slot <= cutoff)
            .copied()
            .collect();

        let mut out = Vec::with_capacity(ready.len());
        for key in ready {
            let Some(st) = self.sets.remove(&key) else { continue };
            out.push(finalize(key, st, &self.truth));
        }

        // The truth entries for those slots have now done their job. Every set at
        // or below the cutoff has been finalized (for every provider — the cutoff
        // does not depend on who sent what), so nothing left can still need them.
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

fn finalize(
    (provider, slot, fec_set_index): (ProviderId, u64, u32),
    st: SetState,
    truth: &AHashMap<(u64, u32, bool, u32), [u8; 32]>,
) -> SetRow {
    // Split the failures. A shred that failed verification either carries the
    // leader's real data behind a broken proof (a relay bug), or carries content
    // the leader never signed (a substitution). Only a leader-authenticated copy
    // of the same shred can tell them apart; absent one, we say so rather than
    // guess.
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
    let expected_total = match (st.num_data, st.num_coding) {
        (Some(d), Some(c)) => Some(d as u32 + c as u32),
        _ => None,
    };
    let delivered = n_data + n_code;
    let missed = expected_total.unwrap_or(delivered).saturating_sub(delivered);

    // The set is reconstructable the instant `num_data` shreds have arrived — any
    // k of the k+m, by Reed-Solomon. `num_data` can be learned late (from a coding
    // shred's header or the DATA_COMPLETE flag), so the decisive shred cannot be
    // stamped live; take the k-th smallest arrival now that k is settled. This is
    // the true moment the set became decodable, independent of when we learned k.
    let decode_ns = st.num_data.and_then(|nd| {
        let k = nd as usize;
        if k == 0 || st.arrivals.len() < k {
            return None;
        }
        let mut arrivals = st.arrivals.clone();
        arrivals.select_nth_unstable(k - 1);
        Some(arrivals[k - 1])
    });

    // A provider "delivered" this set iff it shipped nothing invalid, nothing
    // unverifiable, and enough shreds to reconstruct it.
    let is_valid = invalid == 0 && st.unverifiable == 0 && decode_ns.is_some();

    SetRow {
        provider,
        slot,
        fec_set_index,
        leader: st.leader,
        // Accepted-shred timing. Falls back to the creating shred's arrival only
        // when no shred was ever accepted (all-invalid set) — such a row is
        // `is_valid = false` and never enters a timing comparison.
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

    /// Build a minimal accepted (valid) data shred for one FEC set.
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
            leader: None, // set below by caller when a verdict is needed
            sig_ok: Some(true),
            merkle_ok: true,
            payload_hash: hash,
            data_hash: Some(leaf(hash)),
        }
    }

    fn coding(provider: ProviderId, slot: u64, fec: u32, pos: u32, rx: i64, hash: u64, nd: u16) -> VerifiedShred {
        VerifiedShred {
            provider,
            rx_unix_ns: rx,
            slot,
            fec_set_index: fec,
            shred_index: fec + pos,
            is_code: true,
            position: pos,
            last_in_slot: false,
            data_complete: false,
            num_data: Some(nd),
            num_coding: Some(nd),
            leader: None,
            sig_ok: Some(true),
            merkle_ok: true,
            payload_hash: hash,
            data_hash: Some(leaf(hash)),
        }
    }

    /// A stand-in leaf hash. Equal `hash` => equal block data.
    fn leaf(hash: u64) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&hash.to_le_bytes());
        h
    }

    /// A shred that failed verification. `leaf` says what block data it carried:
    /// `Some(h)` reuses the same convention as the valid ones, so passing the
    /// leaf of the genuine shred means "the data is right, the proof is not".
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

    /// Provider 1 relays the leader's real data behind a wrecked merkle proof —
    /// exactly the failure a real provider was caught doing. Provider 0 delivers
    /// the same shred correctly, and its verified signature is what makes its
    /// leaf the ground truth. The bad copy must land in `invalid_sig`, and must
    /// NOT be accused of altering data.
    #[test]
    fn broken_proof_over_genuine_data_is_invalid_sig_not_invalid_data() {
        let mut agg = Aggregator::new(0);
        // The good copy (provider 0) and the bad copy (provider 1) of shred pos 3.
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

    /// The serious case: the failing shred's data differs from the leader-signed
    /// copy. This is content substitution, not a mangled proof, and it must never
    /// be filed under `invalid_sig`.
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

    /// No provider delivered a leader-authenticated copy of this shred, so there
    /// is nothing to compare against. We must say "unknown" rather than guess —
    /// silently calling it a signature problem would understate a substitution.
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

    /// The authenticated copy may arrive *after* the bad one — classification is
    /// deferred to finalize precisely so ordering cannot change the verdict.
    #[test]
    fn truth_arriving_after_the_bad_copy_still_classifies_it() {
        let mut agg = Aggregator::new(0);
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xAAAA)))));
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_200, 0xAAAA))); // truth, late

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(r.invalid_sig, 1, "late ground truth must still be applied");
        assert_eq!(r.invalid_unknown, 0);
    }

    /// Data and coding shreds have separate index spaces that overlap: in real
    /// traffic nearly every `(slot, fec_set_index, shred_index)` names *both* a
    /// data shred and a coding shred. If the ground-truth map forgets `is_code`,
    /// a failing data shred gets compared against a coding shred's hash, the two
    /// differ, and a plain broken proof is reported as **altered block data** —
    /// accusing a provider of substituting content when it did no such thing.
    ///
    /// Caught in production, not here: the first live run of the split reported
    /// 15,456 "bad data" shreds that were nothing of the sort.
    #[test]
    fn a_coding_shred_is_not_ground_truth_for_a_data_shred_at_the_same_index() {
        let mut agg = Aggregator::new(0);
        // Both live at shred_index = fec + 3, and the coding one lands last, so a
        // key without `is_code` would leave *its* hash as the truth for pos 3.
        agg.ingest(&with_leader(shred(0, 100, 0, 3, 1_000, 0xAAAA)));
        agg.ingest(&with_leader(coding(0, 100, 0, 3, 1_010, 0xCCCC, 32)));
        // Provider 1's data shred carries the leader's real data (0xAAAA) behind a
        // broken proof.
        agg.ingest(&with_leader(bad(1, 100, 0, 3, 1_100, Some(leaf(0xAAAA)))));

        let rows = agg.harvest(true);
        let r = rows.iter().find(|r| r.provider == 1).unwrap();
        assert_eq!(
            r.invalid_data, 0,
            "a coding shred's hash must never be the yardstick for a data shred — that \
             turns a broken proof into a false accusation of content substitution"
        );
        assert_eq!(r.invalid_sig, 1, "the block data matched the leader-signed data shred");
    }

    /// Some providers forward data shreds only. The data-shred count normally
    /// comes from a coding shred's header, so without another source such a
    /// provider could never reach `decode_ns`: every set would be `is_valid =
    /// false`, its winrate would be 0%, and it would look broken while actually
    /// delivering every shred needed to reconstruct the block. The DATA_COMPLETE
    /// flag on the set's last data shred is that other source.
    #[test]
    fn a_data_only_provider_can_still_decode_a_set() {
        let mut agg = Aggregator::new(0);
        // 4 data shreds, no coding shreds at all; the last one closes the set.
        for pos in 0..4u32 {
            let mut s = with_leader(shred(0, 100, 0, pos, 1_000 + pos as i64, 0xA000 + pos as u64));
            s.data_complete = pos == 3;
            agg.ingest(&s);
        }

        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.n_data, 4);
        assert_eq!(r.n_code, 0, "this provider sent no coding shreds");
        assert!(
            r.decode_ns.is_some(),
            "a data-only provider that delivered every data shred must be able to decode"
        );
        assert!(r.is_valid, "and must count as a valid delivery, not a failure");
    }

    /// The three buckets must always account for exactly the total.
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

    fn with_leader(mut s: VerifiedShred) -> VerifiedShred {
        s.leader = Some(Pubkey::new_unique());
        s
    }

    // Regression test for the `first_ns` poisoning bug: an INVALID shred that
    // arrives before any valid shred must NOT pull `first_ns` earlier.
    #[test]
    fn invalid_first_shred_does_not_move_first_ns() {
        let mut agg = Aggregator::new(10);
        // invalid shred arrives first, at t=100
        let mut bad = with_leader(shred(0, 5, 0, 0, 100, 0xAA));
        bad.sig_ok = Some(false);
        agg.ingest(&bad);
        // a valid shred of the same set arrives later, at t=200
        agg.ingest(&with_leader(shred(0, 5, 0, 1, 200, 0xBB)));

        let rows = agg.harvest(true);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        // first_ns must reflect the VALID shred (200), never the invalid one (100)
        assert_eq!(r.first_ns, 200, "invalid shred at t=100 poisoned first_ns");
        assert_eq!(r.invalid, 1);
    }

    #[test]
    fn decode_ns_is_the_kth_arrival_not_when_num_data_was_learned() {
        let mut agg = Aggregator::new(10);
        // One data shred makes the set decodable (num_data turns out to be 1) at
        // t=1000. The coding shred at t=1100 only *reveals* num_data; it does not
        // change when the set became reconstructable. decode_ns must be 1000.
        agg.ingest(&with_leader(shred(0, 7, 0, 0, 1000, 1)));
        agg.ingest(&with_leader(coding(0, 7, 0, 0, 1100, 2, 1)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert!(r.is_valid, "set with enough data shreds should be valid");
        assert_eq!(
            r.decode_ns,
            Some(1000),
            "decode_ns is the k-th arrival (t=1000), not the arrival that revealed num_data (t=1100)"
        );
        assert_eq!(r.first_ns, 1000);
        assert_eq!(r.last_ns, 1100);
    }

    /// The hard case the k-th-arrival rule exists for: all `num_data` data shreds
    /// land in a burst, but `num_data` is only learned from a coding shred that
    /// trickles in much later. The set was decodable when the last data shred
    /// arrived, and `decode_ns` must reflect that, not the late coding shred.
    #[test]
    fn decode_ns_when_num_data_is_learned_only_from_a_late_coding_shred() {
        let mut agg = Aggregator::new(10);
        // 3 data shreds (no DATA_COMPLETE, so num_data stays unknown), arriving at
        // 1000/1001/1002 — the set is decodable at 1002.
        for pos in 0..3u32 {
            agg.ingest(&with_leader(shred(0, 8, 0, pos, 1000 + pos as i64, 0xD000 + pos as u64)));
        }
        // A coding shred at t=5000 finally declares num_data = 3.
        agg.ingest(&with_leader(coding(0, 8, 0, 0, 5000, 0xC0DE, 3)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert!(r.is_valid);
        assert_eq!(
            r.decode_ns,
            Some(1002),
            "decodable at the 3rd data shred (t=1002), not when the late coding shred revealed k"
        );
    }

    #[test]
    fn duplicate_does_not_extend_last_ns() {
        let mut agg = Aggregator::new(10);
        agg.ingest(&with_leader(shred(0, 3, 0, 0, 500, 0x11)));
        // byte-identical retransmit (same payload_hash) arriving much later
        agg.ingest(&with_leader(shred(0, 3, 0, 0, 9999, 0x11)));
        let rows = agg.harvest(true);
        let r = &rows[0];
        assert_eq!(r.duplicated, 1);
        assert_eq!(r.last_ns, 500, "duplicate retransmit must not extend last_ns");
    }

    #[test]
    fn no_leader_is_unverifiable_not_invalid() {
        let mut agg = Aggregator::new(10);
        // merkle_ok but sig_ok = None (no leader) -> unverifiable, not invalid
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
        // not yet past the wait window -> nothing harvested
        assert!(agg.harvest(false).is_empty());
        // advance the max slot well past slot 100
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

    /// A shred arriving for a set that was already finalized must be dropped and
    /// counted, never allowed to resurrect a fresh SetState — otherwise it lands
    /// as a second row for the same key and the viewer's last-wins overwrites the
    /// provider's real, winning row with a one-shred `is_valid = false` straggler.
    #[test]
    fn straggler_after_window_is_dropped_and_counted() {
        let mut agg = Aggregator::new(10);
        // A complete, decodable delivery of set (slot 100, fec 0): one data shred
        // that closes the set, so num_data = 1 and it decodes at t = 1_000.
        let mut good = with_leader(shred(0, 100, 0, 0, 1_000, 0xAAAA));
        good.data_complete = true;
        agg.ingest(&good);

        // Advance the tip well past the window so slot 100 finalizes.
        agg.ingest(&with_leader(shred(0, 200, 0, 0, 2_000, 0xBBBB)));
        let rows = agg.harvest(false);
        let r = rows.iter().find(|r| r.slot == 100).expect("slot 100 finalized");
        assert!(r.is_valid, "the on-time delivery is valid");
        assert_eq!(r.decode_ns, Some(1_000));

        // A straggler for the already-finalized set arrives late.
        agg.ingest(&with_leader(shred(1, 100, 0, 5, 3_000, 0xDEAD)));
        assert_eq!(agg.shreds_after_window(), 1, "the late shred must be counted");

        // It must not have created a new pending set, and thus no second row.
        let rows2 = agg.harvest(true);
        assert!(
            rows2.iter().all(|r| r.slot != 100),
            "a finalized set must never be resurrected by a straggler"
        );
    }
}
