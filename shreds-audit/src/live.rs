//! Live, in-memory provider comparison for the `--tui` dashboard.
//!
//! Consumes finalized `SetRow`s as harvested and accumulates, per provider, the
//! same head-to-head numbers the offline viewer computes — winrate, mean
//! microseconds behind the fastest, and coverage — cumulatively over the run.
//!
//! Keeps no per-set history: O(providers) memory, O(1) per set, so it runs for
//! days without growing. Reports *comparison only* — invalid / bad-signature /
//! bad-data counts are never surfaced here; those belong to the archive and
//! offline viewer. A live glance is for "who is fastest", not for accusations.

use ahash::AHashMap;

use crate::{agg::SetRow, registry::ProviderId};

#[derive(Default, Clone, Copy)]
pub struct ProviderLive {
    /// Sets this provider had any row in.
    pub present: u64,
    /// Sets it delivered validly (decodable, nothing invalid or unverifiable).
    pub valid: u64,
    /// Contested sets (>= 2 valid providers) it entered as a valid deliverer.
    pub races: u64,
    /// Contested sets it decoded first (a tie counts as a win for each tied one).
    pub wins: u64,
    /// Sum of microseconds behind the fastest, over the contested sets it entered.
    delta_sum_us: f64,
    /// Number of contested-set deltas summed — the denominator for the mean.
    delta_n: u64,
}

impl ProviderLive {
    pub fn winrate(&self) -> Option<f64> {
        (self.races > 0).then(|| self.wins as f64 / self.races as f64)
    }
    pub fn mean_behind_us(&self) -> Option<f64> {
        (self.delta_n > 0).then(|| self.delta_sum_us / self.delta_n as f64)
    }
    pub fn coverage(&self, total_sets: u64) -> Option<f64> {
        (total_sets > 0).then(|| self.present as f64 / total_sets as f64)
    }
}

#[derive(Default)]
pub struct LiveStats {
    per_provider: AHashMap<ProviderId, ProviderLive>,
    total_sets: u64,
    contested_sets: u64,
}

impl LiveStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn provider(&self, id: ProviderId) -> ProviderLive {
        self.per_provider.get(&id).copied().unwrap_or_default()
    }

    pub fn total_sets(&self) -> u64 {
        self.total_sets
    }

    pub fn contested_sets(&self) -> u64 {
        self.contested_sets
    }

    /// Fold one harvest's rows in. Every provider's row for a given
    /// `(slot, fec_set_index)` finalizes together — the harvest cutoff is
    /// slot-based, not per-provider — so grouping within a single batch sees the
    /// whole race and never scores half of it.
    pub fn ingest(&mut self, rows: &[SetRow]) {
        let mut groups: AHashMap<(u64, u32), Vec<&SetRow>> = AHashMap::new();
        for r in rows {
            groups.entry((r.slot, r.fec_set_index)).or_default().push(r);
        }
        for set in groups.values() {
            self.fold_set(set);
        }
    }

    fn fold_set(&mut self, set: &[&SetRow]) {
        self.total_sets += 1;

        // Presence is over every row, valid or not — it is "did this provider show
        // up for this set", the denominator of coverage.
        for r in set {
            self.per_provider.entry(r.provider).or_default().present += 1;
        }

        // The fastest valid decode is the reference everyone is measured against.
        let mut win_ns: Option<i64> = None;
        let mut valid_count = 0u32;
        for r in set {
            if r.is_valid {
                if let Some(d) = r.decode_ns {
                    valid_count += 1;
                    win_ns = Some(win_ns.map_or(d, |w| w.min(d)));
                    self.per_provider.entry(r.provider).or_default().valid += 1;
                }
            }
        }

        // A race needs at least two valid deliverers. A set only one provider
        // decoded is not a contest, so it moves no winrate and — crucially — no
        // delta: counting a solo "win" at 0 µs behind would flatter whoever most
        // often delivered alone.
        let contested = valid_count >= 2;
        if !contested {
            return;
        }
        self.contested_sets += 1;
        let win_ns = win_ns.expect("contested implies a valid decode");

        for r in set {
            if !r.is_valid {
                continue;
            }
            let Some(d) = r.decode_ns else { continue };
            let e = self.per_provider.entry(r.provider).or_default();
            e.races += 1;
            if d == win_ns {
                e.wins += 1;
            }
            e.delta_sum_us += (d - win_ns) as f64 / 1000.0;
            e.delta_n += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(provider: ProviderId, slot: u64, fec: u32, decode_ns: Option<i64>, is_valid: bool) -> SetRow {
        SetRow {
            provider,
            slot,
            fec_set_index: fec,
            leader: None,
            first_ns: 0,
            decode_ns,
            last_ns: 0,
            n_data: 0,
            n_code: 0,
            expected_total: None,
            missed: 0,
            invalid: 0,
            invalid_sig: 0,
            invalid_data: 0,
            invalid_unknown: 0,
            duplicated: 0,
            sig_unverifiable: 0,
            is_valid,
            last_in_slot: false,
        }
    }

    #[test]
    fn faster_provider_wins_and_slower_is_behind() {
        let mut s = LiveStats::new();
        // One contested set: provider 0 decodes at 1.000 ms, provider 1 at 1.500 ms.
        s.ingest(&[
            row(0, 10, 0, Some(1_000_000), true),
            row(1, 10, 0, Some(1_500_000), true),
        ]);

        assert_eq!(s.total_sets(), 1);
        assert_eq!(s.contested_sets(), 1);

        let p0 = s.provider(0);
        let p1 = s.provider(1);
        assert_eq!(p0.winrate(), Some(1.0));
        assert_eq!(p1.winrate(), Some(0.0));
        assert_eq!(p0.mean_behind_us(), Some(0.0));
        assert_eq!(p1.mean_behind_us(), Some(500.0), "500_000 ns behind == 500 µs");
        assert_eq!(p0.coverage(s.total_sets()), Some(1.0));
    }

    #[test]
    fn tie_is_a_win_for_both() {
        let mut s = LiveStats::new();
        s.ingest(&[
            row(0, 10, 0, Some(2_000_000), true),
            row(1, 10, 0, Some(2_000_000), true),
        ]);
        assert_eq!(s.provider(0).winrate(), Some(1.0));
        assert_eq!(s.provider(1).winrate(), Some(1.0));
    }

    #[test]
    fn a_solo_set_is_not_a_race_and_does_not_dilute_the_mean() {
        let mut s = LiveStats::new();
        // A contested set first, so provider 0 has one real race.
        s.ingest(&[
            row(0, 10, 0, Some(1_000_000), true),
            row(1, 10, 0, Some(1_500_000), true),
        ]);
        // Then a set only provider 0 delivered validly.
        s.ingest(&[row(0, 11, 0, Some(2_000_000), true)]);

        let p0 = s.provider(0);
        assert_eq!(s.contested_sets(), 1, "the solo set is not a contest");
        assert_eq!(p0.races, 1, "the solo set added no race");
        assert_eq!(p0.present, 2, "but it does count toward coverage");
        assert_eq!(p0.mean_behind_us(), Some(0.0), "solo 0-delta must not be folded in");
    }

    #[test]
    fn invalid_row_counts_as_presence_but_never_a_race() {
        let mut s = LiveStats::new();
        s.ingest(&[
            row(0, 10, 0, Some(1_000_000), true),
            row(1, 10, 0, None, false), // present but did not deliver
        ]);
        let p1 = s.provider(1);
        assert_eq!(p1.present, 1);
        assert_eq!(p1.valid, 0);
        assert_eq!(p1.winrate(), None, "no races entered");
        assert_eq!(s.contested_sets(), 0, "only one valid deliverer => not a race");
    }
}
