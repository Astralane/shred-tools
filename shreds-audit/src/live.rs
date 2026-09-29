use ahash::AHashMap;

use crate::{agg::SetRow, registry::ProviderId};

#[derive(Default, Clone, Copy)]
pub struct ProviderLive {
    pub present: u64,
    pub valid: u64,
    pub races: u64,
    /// A tie counts as a win for each tied provider.
    pub wins: u64,
    delta_sum_us: f64,
    delta_n: u64,
    delta_max_us: f64,
}

impl ProviderLive {
    pub fn winrate(&self) -> Option<f64> {
        (self.races > 0).then(|| self.wins as f64 / self.races as f64)
    }
    pub fn mean_behind_us(&self) -> Option<f64> {
        (self.delta_n > 0).then(|| self.delta_sum_us / self.delta_n as f64)
    }
    pub fn behind_sum_us(&self) -> f64 {
        self.delta_sum_us
    }
    pub fn behind_n(&self) -> u64 {
        self.delta_n
    }
    pub fn behind_max_us(&self) -> f64 {
        self.delta_max_us
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

    /// All providers' rows for a set finalize in the same harvest, so grouping
    /// within one batch always sees the whole race.
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
        for r in set {
            self.per_provider.entry(r.provider).or_default().present += 1;
        }

        let decoded: Vec<(ProviderId, i64)> = set
            .iter()
            .filter(|r| r.is_valid)
            .filter_map(|r| Some((r.provider, r.decode_ns?)))
            .collect();
        for &(provider, _) in &decoded {
            self.per_provider.entry(provider).or_default().valid += 1;
        }

        // A solo delivery is not a race; scoring it would flatter lone deliverers.
        if decoded.len() < 2 {
            return;
        }
        self.contested_sets += 1;
        let win_ns = decoded.iter().map(|&(_, d)| d).min().unwrap();
        for &(provider, d) in &decoded {
            let e = self.per_provider.entry(provider).or_default();
            e.races += 1;
            if d == win_ns {
                e.wins += 1;
            }
            let behind_us = (d - win_ns) as f64 / 1000.0;
            e.delta_sum_us += behind_us;
            e.delta_n += 1;
            e.delta_max_us = e.delta_max_us.max(behind_us);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        provider: ProviderId,
        slot: u64,
        fec: u32,
        decode_ns: Option<i64>,
        is_valid: bool,
    ) -> SetRow {
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
        assert_eq!(p1.behind_max_us(), 500.0);
        assert_eq!(p0.behind_max_us(), 0.0, "the winner is never behind itself");
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
        s.ingest(&[
            row(0, 10, 0, Some(1_000_000), true),
            row(1, 10, 0, Some(1_500_000), true),
        ]);
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
            row(1, 10, 0, None, false),
        ]);
        let p1 = s.provider(1);
        assert_eq!(p1.present, 1);
        assert_eq!(p1.valid, 0);
        assert_eq!(p1.winrate(), None, "no races entered");
        assert_eq!(s.contested_sets(), 0, "only one valid deliverer => not a race");
    }
}
