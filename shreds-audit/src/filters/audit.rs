use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

use ahash::AHashMap;

use super::{bundles::Bundle, db::ViolationRow, spec::TxView};
use crate::sigreg::{Reservoir, SigRegistry, SourceKind};

pub type WindowKey = (usize, u32);

const START_MARGIN: u64 = 2;
const END_MARGIN: u64 = 3;
const VIOLATIONS_PER_KIND: u32 = 25;
const SERVER_DELAY_RESERVOIR: usize = 50_000;
const START_WITHOUT_DELIVERY_SLOTS: u64 = 10;
/// A window's legitimate range is a minute or so of slots. A cursor further
/// behind the tip than this means its start was wrong, and walking it forward
/// would mean a getBlock for every sampled slot of chain history in between.
const MAX_CURSOR_LAG_SLOTS: u64 = 600;
/// Slots handed out per window per tick, so one window can never monopolise the
/// checker.
const MAX_TASKS_PER_WINDOW: usize = 32;

fn commitment_lag(kind: SourceKind, commitment: Option<&str>) -> u64 {
    if kind == SourceKind::GrpcDeshred {
        return 0;
    }
    match commitment.map(str::to_lowercase).as_deref() {
        Some("confirmed") => 4,
        Some("finalized") => 40,
        _ => 0,
    }
}

pub struct WindowInfo {
    pub source: String,
    pub kind: SourceKind,
    pub commitment: Option<String>,
    pub bundle: Arc<Bundle>,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct CheckTotals {
    pub expected: u64,
    pub delivered: u64,
    pub matched: u64,
    pub missed: u64,
    pub extra_in_block: u64,
    pub extra_not_in_block: u64,
}

impl CheckTotals {
    pub fn add(&mut self, o: &CheckTotals) {
        self.expected += o.expected;
        self.delivered += o.delivered;
        self.matched += o.matched;
        self.missed += o.missed;
        self.extra_in_block += o.extra_in_block;
        self.extra_not_in_block += o.extra_not_in_block;
    }
}

pub struct Window {
    pub key: WindowKey,
    pub info: WindowInfo,
    commitment_lag: u64,
    pub started_at: SystemTime,
    opened: Instant,
    pub connect_ms: Option<f64>,
    pub first_msg_ms: Option<f64>,
    pub tip_open: Option<u64>,
    /// The subscription is live. `tip_open` can still be `None` after this when
    /// it went live before any slot was seen (startup): it is then filled from
    /// the first real tip or delivery, which is later than the subscription and
    /// so on the safe side.
    pub subscribed: bool,
    pub tip_close: Option<u64>,
    pub ended_at: Option<SystemTime>,
    pub end_reason: Option<String>,

    pub delivered: u64,
    pub first_delivered_slot: Option<u64>,
    pub max_delivered_slot: u64,
    pub untagged_updates: u64,
    pub unknown_tags: u64,
    pub duplicates: u64,
    pub tagged: Vec<u64>,
    pub tag_false_positive: Vec<u64>,
    pub tag_missing: Vec<u64>,
    pub vote_flag_mismatch: u64,
    pub server_delay_us: Reservoir,

    sampled: BTreeMap<u64, AHashMap<[u8; 64], u32>>,
    next_check: Option<u64>,
    pub in_flight: u32,
    pub totals: Vec<CheckTotals>,
    pub slots_checked: u64,
    pub slots_skipped: u64,
    pub slots_unchecked: u64,
    pub audit_start: Option<u64>,
    pub audit_end: Option<u64>,

    violations: Vec<ViolationRow>,
    violation_counts: AHashMap<(u16, &'static str), u32>,
}

impl Window {
    fn new(key: WindowKey, info: WindowInfo) -> Self {
        let n = info.bundle.filters.len();
        Self {
            key,
            commitment_lag: commitment_lag(info.kind, info.commitment.as_deref()),
            info,
            started_at: SystemTime::now(),
            opened: Instant::now(),
            connect_ms: None,
            first_msg_ms: None,
            tip_open: None,
            subscribed: false,
            tip_close: None,
            ended_at: None,
            end_reason: None,
            delivered: 0,
            first_delivered_slot: None,
            max_delivered_slot: 0,
            untagged_updates: 0,
            unknown_tags: 0,
            duplicates: 0,
            tagged: vec![0; n],
            tag_false_positive: vec![0; n],
            tag_missing: vec![0; n],
            vote_flag_mismatch: 0,
            server_delay_us: Reservoir::new(SERVER_DELAY_RESERVOIR),
            sampled: BTreeMap::new(),
            next_check: None,
            in_flight: 0,
            totals: vec![CheckTotals::default(); n],
            slots_checked: 0,
            slots_skipped: 0,
            slots_unchecked: 0,
            audit_start: None,
            audit_end: None,
            violations: Vec::new(),
            violation_counts: AHashMap::new(),
        }
    }

    fn start_slot(&self) -> Option<u64> {
        let tip = self.tip_open?;
        Some(tip.max(self.first_delivered_slot.unwrap_or(0)) + START_MARGIN)
    }

    fn start_known(&self, tip: u64) -> bool {
        self.first_delivered_slot.is_some()
            || self.ended_at.is_some()
            || self
                .tip_open
                .is_some_and(|t| tip >= t + START_WITHOUT_DELIVERY_SLOTS)
    }

    fn end_slot(&self, tip: u64) -> u64 {
        let end = self
            .tip_close
            .unwrap_or(tip)
            .saturating_sub(END_MARGIN + self.commitment_lag);
        if self.delivered > 0 {
            end.min(self.max_delivered_slot.saturating_sub(1))
        } else {
            end
        }
    }

    pub fn violation(
        &mut self,
        filter: Option<usize>,
        kind: &'static str,
        slot: u64,
        signature: &[u8; 64],
        reason: String,
    ) {
        let fid = filter.map_or(u16::MAX, |f| f as u16);
        let n = self.violation_counts.entry((fid, kind)).or_insert(0);
        if *n >= VIOLATIONS_PER_KIND {
            return;
        }
        *n += 1;
        self.violations.push(ViolationRow {
            ts: SystemTime::now(),
            source: self.info.source.clone(),
            kind: self.info.kind.label(),
            window_started_at: self.started_at,
            bundle: self.info.bundle.name.clone(),
            filter: filter.map_or_else(|| "*".into(), |f| self.info.bundle.filters[f].name.clone()),
            slot,
            signature: bs58::encode(signature).into_string(),
            violation: kind,
            reason,
        });
    }

    pub fn duration_secs(&self) -> f64 {
        let end = self.ended_at.unwrap_or_else(SystemTime::now);
        end.duration_since(self.started_at)
            .map_or(0.0, |d| d.as_secs_f64())
    }

    fn finished(mut self) -> Self {
        self.slots_unchecked += self.in_flight as u64;
        self
    }
}

pub struct SlotTask {
    pub key: WindowKey,
    pub slot: u64,
    pub bundle: Arc<Bundle>,
    pub deliveries: AHashMap<[u8; 64], u32>,
    pub attempts: u32,
}

pub struct FilterAudit {
    state: Mutex<AHashMap<WindowKey, Window>>,
    sample_every: u64,
    skip_votes: bool,
    reg: Arc<Mutex<SigRegistry>>,
}

impl FilterAudit {
    pub fn new(sample_every: u64, skip_votes: bool, reg: Arc<Mutex<SigRegistry>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AHashMap::new()),
            sample_every,
            skip_votes,
            reg,
        })
    }

    fn tip(&self) -> u64 {
        self.reg.lock().unwrap().high_slot()
    }

    pub fn open(&self, key: WindowKey, info: WindowInfo) {
        self.state
            .lock()
            .unwrap()
            .insert(key, Window::new(key, info));
    }

    pub fn subscribed(&self, key: WindowKey) {
        let tip = self.tip();
        if let Some(w) = self.state.lock().unwrap().get_mut(&key) {
            // Tip 0 is "no slot seen yet", not slot 0: starting the audited range
            // there would walk the checker through the whole chain history.
            w.tip_open = (tip > 0).then_some(tip);
            w.subscribed = true;
            w.connect_ms = Some(w.opened.elapsed().as_secs_f64() * 1000.0);
        }
    }

    pub fn first_message(&self, key: WindowKey) {
        if let Some(w) = self.state.lock().unwrap().get_mut(&key) {
            if w.first_msg_ms.is_none() {
                w.first_msg_ms = Some(w.opened.elapsed().as_secs_f64() * 1000.0);
            }
        }
    }

    pub fn close(&self, key: WindowKey, reason: String) {
        let tip = self.tip();
        if let Some(w) = self.state.lock().unwrap().get_mut(&key) {
            w.tip_close = Some(tip);
            w.ended_at = Some(SystemTime::now());
            w.end_reason = Some(reason);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_delivery(
        &self,
        key: WindowKey,
        slot: u64,
        tx: &TxView,
        tags: &[String],
        server_is_vote: Option<bool>,
        rx_ns: i64,
        created_ns: Option<i64>,
    ) {
        let mut state = self.state.lock().unwrap();
        let Some(w) = state.get_mut(&key) else { return };
        let bundle = w.info.bundle.clone();
        let n = bundle.filters.len();

        w.delivered += 1;
        w.first_delivered_slot.get_or_insert(slot);
        if w.subscribed && w.tip_open.is_none() && slot > 0 {
            w.tip_open = Some(slot);
        }
        w.max_delivered_slot = w.max_delivered_slot.max(slot);
        if let Some(c) = created_ns {
            w.server_delay_us.push((rx_ns - c) / 1000);
        }

        let local: Vec<bool> = bundle.filters.iter().map(|f| f.matches(tx)).collect();
        let local_mask = local
            .iter()
            .enumerate()
            .fold(0u32, |m, (i, &hit)| m | ((hit as u32) << i));

        // Without tags a multi-filter bundle falls back to local matching, which can't be blamed.
        let inferred = tags.is_empty() && n > 1;
        let mask = if inferred {
            w.untagged_updates += 1;
            local_mask
        } else if tags.is_empty() {
            1
        } else {
            let mut m = 0u32;
            for t in tags {
                match bundle.index_of(t) {
                    Some(i) => m |= 1 << i,
                    None => w.unknown_tags += 1,
                }
            }
            m
        };

        for (i, f) in bundle.filters.iter().enumerate() {
            let tagged = mask >> i & 1 == 1;
            if tagged {
                w.tagged[i] += 1;
            }
            if inferred {
                continue;
            }
            if tagged && !local[i] {
                w.tag_false_positive[i] += 1;
                let why = f.mismatch(tx).unwrap_or_default();
                w.violation(Some(i), "tag_false_positive", slot, &tx.signature, why);
            } else if !tagged && local[i] {
                w.tag_missing[i] += 1;
                let why = f.match_reason(tx);
                w.violation(Some(i), "tag_missing", slot, &tx.signature, why);
            }
        }

        if let Some(server) = server_is_vote {
            let ours = tx.is_vote();
            if server != ours {
                w.vote_flag_mismatch += 1;
                let why = format!(
                    "server is_vote={server}, simple-vote rule={ours} (signatures={}, legacy={}, \
                     instructions={})",
                    tx.num_signatures, tx.legacy, tx.num_instructions
                );
                w.violation(None, "vote_flag_mismatch", slot, &tx.signature, why);
            }
        }

        let sampled = !(self.skip_votes && tx.is_vote())
            && slot.is_multiple_of(self.sample_every)
            && w.start_slot().is_some_and(|s| slot >= s);
        if sampled && w.sampled.entry(slot).or_default().insert(tx.signature, mask).is_some() {
            w.duplicates += 1;
            w.violation(
                None,
                "duplicate",
                slot,
                &tx.signature,
                "delivered more than once in this window".into(),
            );
        }
    }

    pub fn take_ready(&self, lag_slots: u64) -> Vec<SlotTask> {
        let tip = self.tip();
        let n = self.sample_every;
        let mut out = Vec::new();
        let mut state = self.state.lock().unwrap();
        for w in state.values_mut() {
            if w.subscribed && w.tip_open.is_none() && tip > 0 {
                w.tip_open = Some(tip);
            }
            if w.next_check.is_none() && !w.start_known(tip) {
                continue;
            }
            let Some(start) = w.start_slot() else {
                continue;
            };
            let end = w.end_slot(tip);
            let next = w.next_check.get_or_insert(start.div_ceil(n) * n);
            let floor = tip.saturating_sub(lag_slots + MAX_CURSOR_LAG_SLOTS);
            if *next < floor {
                let jumped = floor.div_ceil(n) * n;
                // Never silent: these sampled slots go on record as unchecked.
                w.slots_unchecked += (jumped - *next) / n;
                *next = jumped;
            }
            let mut handed = 0;
            while *next <= end && *next + lag_slots <= tip && handed < MAX_TASKS_PER_WINDOW {
                handed += 1;
                let slot = *next;
                *next += n;
                out.push(SlotTask {
                    key: w.key,
                    slot,
                    bundle: w.info.bundle.clone(),
                    deliveries: w.sampled.remove(&slot).unwrap_or_default(),
                    attempts: 0,
                });
                w.in_flight += 1;
                w.audit_start.get_or_insert(slot);
                w.audit_end = Some(slot);
            }
            let cursor = *next;
            w.sampled.retain(|&s, _| s >= cursor);
        }
        out
    }

    pub fn with_window<R>(&self, key: WindowKey, f: impl FnOnce(&mut Window) -> R) -> Option<R> {
        self.state.lock().unwrap().get_mut(&key).map(f)
    }

    pub fn drain_violations(&self) -> Vec<ViolationRow> {
        self.state
            .lock()
            .unwrap()
            .values_mut()
            .flat_map(|w| std::mem::take(&mut w.violations))
            .collect()
    }

    pub fn take_finished(&self, give_up_secs: u64) -> Vec<Window> {
        let (tip, floor) = {
            let reg = self.reg.lock().unwrap();
            (reg.high_slot(), reg.finalized_floor())
        };
        let mut state = self.state.lock().unwrap();
        let done: Vec<WindowKey> = state
            .values()
            .filter(|w| {
                let Some(ended) = w.ended_at else {
                    return false;
                };
                let checks_done = w.in_flight == 0
                    && match (w.start_slot(), w.next_check) {
                        (None, _) => true,
                        (Some(_), Some(next)) => next > w.end_slot(tip),
                        (Some(start), None) => start > w.end_slot(tip),
                    };
                let latency_final = w.delivered == 0 || w.tip_close.is_none_or(|t| floor > t);
                let stale = ended.elapsed().is_ok_and(|e| e.as_secs() >= give_up_secs);
                (checks_done && latency_final) || stale
            })
            .map(|w| w.key)
            .collect();
        done.into_iter()
            .filter_map(|k| state.remove(&k))
            .map(Window::finished)
            .collect()
    }

    pub fn take_all(&self) -> Vec<Window> {
        self.state
            .lock()
            .unwrap()
            .drain()
            .map(|(_, w)| w.finished())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::{bundles, spec::Key};
    use crate::sigreg::SourceKind;

    fn reg_at(tip: u64) -> Arc<Mutex<SigRegistry>> {
        let reg = Arc::new(Mutex::new(SigRegistry::new(
            vec!["s".into(), "g".into()],
            vec![SourceKind::Shred, SourceKind::GrpcDeshred],
        )));
        bump(&reg, tip);
        reg
    }

    fn bump(reg: &Arc<Mutex<SigRegistry>>, slot: u64) {
        let mut sig = [0u8; 64];
        sig[..8].copy_from_slice(&slot.to_le_bytes());
        reg.lock()
            .unwrap()
            .record_first(0, sig, 1, slot, Default::default());
    }

    fn bundle(name: &str) -> Arc<Bundle> {
        let cfg = bundles::defaults()
            .into_iter()
            .find(|b| b.name == name)
            .unwrap();
        Arc::new(Bundle::new(&cfg).unwrap())
    }

    fn info(b: Arc<Bundle>) -> WindowInfo {
        WindowInfo {
            source: "g".into(),
            kind: SourceKind::GrpcDeshred,
            commitment: None,
            bundle: b,
        }
    }

    fn user_tx(sig: u8) -> TxView {
        let k: Key = [7; 32];
        TxView {
            signature: [sig; 64],
            num_signatures: 1,
            legacy: true,
            keys: vec![[1; 32], k],
            num_static_keys: 2,
            num_instructions: 2,
            first_program: Some(k),
            failed: None,
        }
    }

    #[test]
    fn only_fully_covered_confirmed_sampled_slots_are_handed_out() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        audit.on_delivery(key, 1003, &user_tx(4), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1010, &user_tx(1), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1005, &user_tx(2), &["all".into()], None, 0, None);
        bump(&reg, 1030);
        audit.on_delivery(key, 1030, &user_tx(3), &["all".into()], None, 0, None);
        assert!(audit.take_ready(32).is_empty(), "1010 is not confirmed yet");
        bump(&reg, 1042);
        let ready = audit.take_ready(32);
        assert_eq!(ready.iter().map(|t| t.slot).collect::<Vec<_>>(), vec![1010]);
        assert_eq!(ready[0].deliveries.len(), 1, "1005 was never sampled");
    }

    #[test]
    fn a_closed_window_stops_before_its_last_delivered_slot() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        audit.on_delivery(key, 1001, &user_tx(2), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1020, &user_tx(1), &["all".into()], None, 0, None);
        bump(&reg, 1021);
        audit.close(key, "rotated".into());
        bump(&reg, 1060);
        let slots: Vec<u64> = audit.take_ready(32).iter().map(|t| t.slot).collect();
        assert_eq!(slots, vec![1010]);
    }

    #[test]
    fn a_stale_tip_at_subscribe_time_does_not_start_the_range_early() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        assert!(
            audit.take_ready(32).is_empty(),
            "start unknown until the first delivery"
        );
        audit.on_delivery(key, 1017, &user_tx(1), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1030, &user_tx(2), &["all".into()], None, 0, None);
        bump(&reg, 1100);
        audit.on_delivery(key, 1100, &user_tx(3), &["all".into()], None, 0, None);
        let slots: Vec<u64> = audit.take_ready(32).iter().map(|t| t.slot).collect();
        assert_eq!(
            slots.first(),
            Some(&1020),
            "starts after 1017 + margin, not 1000 + margin"
        );
    }

    #[test]
    fn a_silent_window_is_still_checked_once_the_tip_moves_on() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("combo_never")));
        audit.subscribed(key);
        bump(&reg, 1100);
        let slots: Vec<u64> = audit.take_ready(32).iter().map(|t| t.slot).collect();
        assert_eq!(
            slots.first(),
            Some(&1010),
            "nothing delivered is itself checked"
        );
    }

    #[test]
    fn subscribing_before_any_slot_is_seen_never_starts_at_slot_zero() {
        let reg = Arc::new(Mutex::new(SigRegistry::new(
            vec!["s".into(), "g".into()],
            vec![SourceKind::Shred, SourceKind::GrpcDeshred],
        )));
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("combo_never")));
        audit.subscribed(key); // startup: no shred yet, tip is 0
        bump(&reg, 452_000_000);
        assert!(audit.take_ready(32).is_empty(), "tip just learned; start not settled yet");
        bump(&reg, 452_000_100);
        let slots: Vec<u64> = audit.take_ready(32).iter().map(|t| t.slot).collect();
        assert!(!slots.is_empty());
        assert!(slots.iter().all(|&s| s >= 452_000_000), "{slots:?}");
    }

    #[test]
    fn a_cursor_far_behind_the_tip_jumps_instead_of_walking_history() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg.clone());
        let key = (1, 0);
        audit.open(key, info(bundle("combo_never")));
        audit.subscribed(key);
        bump(&reg, 5_000_000);
        let ready = audit.take_ready(32);
        assert!(ready.len() <= MAX_TASKS_PER_WINDOW);
        assert!(ready[0].slot >= 5_000_000 - 32 - MAX_CURSOR_LAG_SLOTS);
    }

    #[test]
    fn a_tag_the_filter_disagrees_with_is_a_violation() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("votes_split")));
        audit.subscribed(key);
        audit.on_delivery(
            key,
            1001,
            &user_tx(1),
            &["votes".into()],
            Some(false),
            0,
            None,
        );
        let v = audit.drain_violations();
        let kinds: Vec<(&str, &str)> = v.iter().map(|r| (r.filter.as_str(), r.violation)).collect();
        assert!(kinds.contains(&("votes", "tag_false_positive")));
        assert!(kinds.contains(&("non_votes", "tag_missing")));
        audit.with_window(key, |w| {
            assert_eq!(w.tag_false_positive, vec![0, 1]);
            assert_eq!(w.tag_missing, vec![1, 0]);
        });
    }

    #[test]
    fn untagged_updates_in_a_multi_filter_bundle_are_counted_not_blamed() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("votes_split")));
        audit.subscribed(key);
        audit.on_delivery(key, 1001, &user_tx(1), &[], None, 0, None);
        assert!(audit.drain_violations().is_empty());
        audit.with_window(key, |w| assert_eq!(w.untagged_updates, 1));
    }

    #[test]
    fn a_server_vote_flag_that_disagrees_with_the_rule_is_reported() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        audit.on_delivery(key, 1001, &user_tx(1), &["all".into()], Some(true), 0, None);
        audit.with_window(key, |w| assert_eq!(w.vote_flag_mismatch, 1));
    }

    #[test]
    fn repeat_deliveries_in_a_sampled_slot_are_duplicates() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        audit.on_delivery(key, 1001, &user_tx(2), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1010, &user_tx(1), &["all".into()], None, 0, None);
        audit.on_delivery(key, 1010, &user_tx(1), &["all".into()], None, 0, None);
        audit.with_window(key, |w| assert_eq!(w.duplicates, 1));
    }

    #[test]
    fn violations_are_capped_per_kind() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        for i in 0..100u8 {
            audit.on_delivery(key, 1001, &user_tx(i), &["all".into()], Some(true), 0, None);
        }
        assert_eq!(audit.drain_violations().len(), VIOLATIONS_PER_KIND as usize);
        audit.with_window(key, |w| assert_eq!(w.vote_flag_mismatch, 100));
    }

    #[test]
    fn a_window_that_never_subscribed_finishes_without_checks() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.close(key, "error: connect".into());
        assert!(audit.take_ready(32).is_empty());
        assert_eq!(audit.take_finished(600).len(), 1);
    }

    #[test]
    fn a_window_that_delivered_waits_for_its_latency_to_be_folded() {
        let reg = reg_at(1000);
        let audit = FilterAudit::new(10, false, reg);
        let key = (1, 0);
        audit.open(key, info(bundle("all")));
        audit.subscribed(key);
        audit.on_delivery(key, 1001, &user_tx(1), &["all".into()], None, 0, None);
        audit.close(key, "rotated".into());
        assert!(audit.take_finished(600).is_empty());
        audit.reg.lock().unwrap().finalize(true);
        assert_eq!(audit.take_finished(600).len(), 1);
    }
}
