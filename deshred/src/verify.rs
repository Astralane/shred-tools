use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::Deserialize;
use serde_json::{Value, json};
use solana_address::Address;
use solana_ledger::shred::layout;
use solana_signature::Signature;

const EPOCHS_KEPT: usize = 3;
const SLOTS_CACHED: u64 = 64;
const MAX_ROOTS_PER_SLOT: usize = 4_096;
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error("rpc {method}: {source}")]
    Transport {
        method: &'static str,
        source: Box<ureq::Error>,
    },
    #[error("rpc {method}: unreadable response: {source}")]
    Response {
        method: &'static str,
        source: std::io::Error,
    },
    #[error("rpc {method} failed: {error}")]
    Rpc { method: &'static str, error: Value },
    #[error("rpc {method}: unexpected result: {source}")]
    Result {
        method: &'static str,
        source: serde_json::Error,
    },
    #[error("bad leader pubkey {0}")]
    BadPubkey(String),
    #[error("leader schedule from slot {first_slot} covers {covered} of {slots} slots")]
    Incomplete {
        first_slot: u64,
        covered: usize,
        slots: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Valid,
    Invalid,
    UnknownLeader,
}

#[derive(Default)]
pub struct LeaderSchedule {
    epochs: RwLock<BTreeMap<u64, Vec<Address>>>,
}

impl LeaderSchedule {
    pub fn insert_epoch(&self, first_slot: u64, leaders: Vec<Address>) {
        let mut epochs = self.epochs.write().unwrap();
        epochs.insert(first_slot, leaders);
        while epochs.len() > EPOCHS_KEPT {
            epochs.pop_first();
        }
    }

    pub fn leader(&self, slot: u64) -> Option<Address> {
        let epochs = self.epochs.read().unwrap();
        let (first_slot, leaders) = epochs.range(..=slot).next_back()?;
        leaders.get((slot - first_slot) as usize).copied()
    }

    pub fn refresh_from_rpc(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<(), ScheduleError> {
        let rpc = Rpc { url, headers };
        let info: EpochInfo = rpc.call("getEpochInfo", json!([]))?;
        let first_slot = info.absolute_slot - info.slot_index;
        for first_slot in [first_slot, first_slot + info.slots_in_epoch] {
            if self.epochs.read().unwrap().contains_key(&first_slot) {
                continue;
            }
            let schedule: Option<HashMap<String, Vec<u64>>> =
                rpc.call("getLeaderSchedule", json!([first_slot]))?;
            if let Some(schedule) = schedule {
                let leaders = leaders_by_slot(schedule, first_slot, info.slots_in_epoch)?;
                self.insert_epoch(first_slot, leaders);
            }
        }
        Ok(())
    }
}

pub struct ShredVerifier {
    leaders: Arc<LeaderSchedule>,
    roots: RwLock<BTreeMap<u64, HashMap<[u8; 32], bool>>>,
}

impl ShredVerifier {
    pub fn new(leaders: Arc<LeaderSchedule>) -> Self {
        Self {
            leaders,
            roots: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn verify(&self, shred: &[u8]) -> Verdict {
        let (Some(slot), Some(root)) = (layout::get_slot(shred), layout::get_merkle_root(shred))
        else {
            return Verdict::Invalid;
        };
        let root = root.to_bytes();
        let cached = self
            .roots
            .read()
            .unwrap()
            .get(&slot)
            .and_then(|roots| roots.get(&root).copied());
        if let Some(valid) = cached {
            return verdict(valid);
        }

        let Some(leader) = self.leaders.leader(slot) else {
            return Verdict::UnknownLeader;
        };
        let valid = shred
            .get(..64)
            .and_then(|bytes| Signature::try_from(bytes).ok())
            .is_some_and(|signature| signature.verify(leader.as_ref(), &root));

        let mut roots = self.roots.write().unwrap();
        let slot_roots = roots.entry(slot).or_default();
        if slot_roots.len() < MAX_ROOTS_PER_SLOT {
            slot_roots.insert(root, valid);
        }
        if let Some(&newest) = roots.keys().next_back() {
            *roots = roots.split_off(&newest.saturating_sub(SLOTS_CACHED - 1));
        }
        verdict(valid)
    }
}

fn verdict(valid: bool) -> Verdict {
    if valid {
        Verdict::Valid
    } else {
        Verdict::Invalid
    }
}

fn leaders_by_slot(
    schedule: HashMap<String, Vec<u64>>,
    first_slot: u64,
    slots: u64,
) -> Result<Vec<Address>, ScheduleError> {
    let mut leaders = vec![None; slots as usize];
    for (leader, indexes) in schedule {
        let leader: Address = leader
            .parse()
            .map_err(|_| ScheduleError::BadPubkey(leader.clone()))?;
        for index in indexes {
            if let Some(slot) = leaders.get_mut(index as usize) {
                *slot = Some(leader);
            }
        }
    }
    let leaders: Vec<Address> = leaders.into_iter().flatten().collect();
    if leaders.len() != slots as usize {
        return Err(ScheduleError::Incomplete {
            first_slot,
            covered: leaders.len(),
            slots,
        });
    }
    Ok(leaders)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EpochInfo {
    absolute_slot: u64,
    slot_index: u64,
    slots_in_epoch: u64,
}

struct Rpc<'a> {
    url: &'a str,
    headers: &'a [(&'a str, &'a str)],
}

impl Rpc<'_> {
    fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<T, ScheduleError> {
        #[derive(Deserialize)]
        struct Response {
            result: Option<Value>,
            error: Option<Value>,
        }

        let mut request = ureq::post(self.url).timeout(RPC_TIMEOUT);
        for (name, value) in self.headers {
            request = request.set(name, value);
        }
        let response: Response = request
            .send_json(json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .map_err(|source| ScheduleError::Transport {
                method,
                source: Box::new(source),
            })?
            .into_json()
            .map_err(|source| ScheduleError::Response { method, source })?;
        if let Some(error) = response.error {
            return Err(ScheduleError::Rpc { method, error });
        }
        serde_json::from_value(response.result.unwrap_or(Value::Null))
            .map_err(|source| ScheduleError::Result { method, source })
    }
}

#[cfg(test)]
mod tests {
    use solana_entry::entry::Entry;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shred, Shredder};
    use solana_signer::Signer;

    use super::*;

    const SLOT: u64 = 1_000;

    fn shreds(leader: &Keypair) -> (Vec<Shred>, Vec<Shred>) {
        let entries: Vec<Entry> = (0..1_000)
            .map(|_| Entry::new(&Hash::default(), 1, Vec::new()))
            .collect();
        Shredder::new(SLOT, SLOT - 1, 0, 42)
            .unwrap()
            .entries_to_merkle_shreds_for_tests(
                leader,
                &entries,
                true,
                Hash::default(),
                0,
                0,
                &ReedSolomonCache::default(),
                &mut ProcessShredsStats::default(),
            )
    }

    fn verifier(first_slot: u64, leaders: Vec<Address>) -> ShredVerifier {
        let schedule = LeaderSchedule::default();
        schedule.insert_epoch(first_slot, leaders);
        ShredVerifier::new(Arc::new(schedule))
    }

    #[test]
    fn looks_up_the_leader_across_epochs() {
        let (a, b) = (Address::new_unique(), Address::new_unique());
        let schedule = LeaderSchedule::default();
        schedule.insert_epoch(100, vec![a; 10]);
        schedule.insert_epoch(110, vec![b; 10]);

        assert_eq!(schedule.leader(99), None);
        assert_eq!(schedule.leader(109), Some(a));
        assert_eq!(schedule.leader(110), Some(b));
        assert_eq!(schedule.leader(120), None);
    }

    #[test]
    fn accepts_data_and_coding_shreds_signed_by_the_leader() {
        let leader = Keypair::new();
        let (data, coding) = shreds(&leader);
        let verifier = verifier(SLOT, vec![leader.pubkey()]);

        for shred in data.iter().chain(&coding) {
            assert_eq!(verifier.verify(shred.payload()), Verdict::Valid);
        }
    }

    #[test]
    fn rejects_shreds_signed_by_someone_else() {
        let (data, _) = shreds(&Keypair::new());
        let verifier = verifier(SLOT, vec![Keypair::new().pubkey()]);

        assert_eq!(verifier.verify(data[0].payload()), Verdict::Invalid);
        assert_eq!(verifier.verify(data[1].payload()), Verdict::Invalid);
    }

    #[test]
    fn rejects_a_tampered_shred_whose_set_was_already_verified() {
        let leader = Keypair::new();
        let (data, _) = shreds(&leader);
        let verifier = verifier(SLOT, vec![leader.pubkey()]);
        assert_eq!(verifier.verify(data[0].payload()), Verdict::Valid);

        let mut tampered = data[1].payload().to_vec();
        tampered[100] ^= 1;
        assert_eq!(verifier.verify(&tampered), Verdict::Invalid);
    }

    #[test]
    fn reports_an_unknown_leader() {
        let (data, _) = shreds(&Keypair::new());
        let verifier = verifier(SLOT + 1, vec![Keypair::new().pubkey()]);

        assert_eq!(verifier.verify(data[0].payload()), Verdict::UnknownLeader);
    }

    #[test]
    fn builds_the_schedule_from_rpc_output() {
        let (a, b) = (Address::new_unique(), Address::new_unique());
        let schedule = HashMap::from([(a.to_string(), vec![0, 2]), (b.to_string(), vec![1])]);

        assert_eq!(leaders_by_slot(schedule.clone(), 0, 3).unwrap(), [a, b, a]);
        assert!(matches!(
            leaders_by_slot(schedule, 0, 4),
            Err(ScheduleError::Incomplete { covered: 3, .. })
        ));
    }
}
