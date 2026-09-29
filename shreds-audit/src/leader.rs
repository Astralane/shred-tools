//! Leader schedule (slot -> leader pubkey), fetched over JSON-RPC per epoch.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use ahash::{AHashMap, AHashSet};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

use crate::names;
use crate::rpc::RpcEndpoint;

#[derive(Deserialize)]
struct EpochInfo {
    #[serde(rename = "absoluteSlot")]
    absolute_slot: u64,
    #[serde(rename = "slotIndex")]
    slot_index: u64,
    #[serde(rename = "slotsInEpoch")]
    slots_in_epoch: u64,
    epoch: u64,
}

/// Floor for the plausibility window, so a short (test) schedule still admits nearby slots.
const SLOTS_PER_EPOCH: u64 = 432_000;

pub enum SlotVerdict {
    /// Far enough outside the loaded schedule that the datagram is not a shred.
    Implausible,
    Leader(Pubkey),
    /// Plausible but not in the schedule: "cannot verify", never "invalid".
    Unknown,
}

struct Epoch {
    epoch: u64,
    first_slot: u64,
    leaders: Vec<Option<Pubkey>>,
}

pub struct LeaderSchedule {
    rpc: RpcEndpoint,
    inner: RwLock<Option<Epoch>>,
    /// Identity pubkey -> display name; empty until `refresh_names` succeeds.
    names: RwLock<AHashMap<Pubkey, String>>,
}

impl LeaderSchedule {
    pub fn new(rpc: RpcEndpoint) -> Arc<Self> {
        Arc::new(Self {
            rpc,
            inner: RwLock::new(None),
            names: RwLock::new(AHashMap::new()),
        })
    }

    #[cfg(test)]
    pub fn for_test(first_slot: u64, leaders: Vec<Option<Pubkey>>) -> Arc<Self> {
        Arc::new(Self {
            rpc: crate::rpc::RpcCfg::generic("http://unused")
                .resolve()
                .unwrap(),
            inner: RwLock::new(Some(Epoch {
                epoch: 0,
                first_slot,
                leaders,
            })),
            names: RwLock::new(AHashMap::new()),
        })
    }

    pub fn refresh_names(&self) -> Result<()> {
        let map = names::fetch_validator_names()?;
        let n = map.len();
        *self.names.write().unwrap() = map;
        eprintln!("validator names: {n} resolved");
        Ok(())
    }

    /// `pubkey -> name` for the named leaders of the current epoch.
    pub fn leader_names(&self) -> HashMap<String, String> {
        let names = self.names.read().unwrap();
        let guard = self.inner.read().unwrap();
        let Some(e) = guard.as_ref() else {
            return HashMap::new();
        };
        let distinct: AHashSet<Pubkey> = e.leaders.iter().flatten().copied().collect();
        distinct
            .into_iter()
            .filter_map(|pk| names.get(&pk).map(|name| (pk.to_string(), name.clone())))
            .collect()
    }

    pub fn classify(&self, slot: u64) -> SlotVerdict {
        let guard = self.inner.read().unwrap();
        let Some(e) = guard.as_ref() else {
            return SlotVerdict::Unknown;
        };

        // Non-shred datagrams can parse into nonsense slots. The window (an epoch
        // below, two above) is loose so real shreds near a boundary become `Unknown`.
        let span = (e.leaders.len() as u64).max(SLOTS_PER_EPOCH);
        let lo = e.first_slot.saturating_sub(span);
        let hi = e.first_slot.saturating_add(span.saturating_mul(2));
        if slot < lo || slot >= hi {
            return SlotVerdict::Implausible;
        }

        let leader = slot
            .checked_sub(e.first_slot)
            .and_then(|idx| e.leaders.get(idx as usize).copied().flatten());
        match leader {
            Some(pk) => SlotVerdict::Leader(pk),
            None => SlotVerdict::Unknown,
        }
    }

    /// True when `slot` falls outside the epoch we currently hold.
    pub fn needs_refresh(&self, slot: u64) -> bool {
        match self.inner.read().unwrap().as_ref() {
            None => true,
            Some(e) => slot < e.first_slot || slot >= e.first_slot + e.leaders.len() as u64,
        }
    }

    pub fn epoch(&self) -> Option<u64> {
        self.inner.read().unwrap().as_ref().map(|e| e.epoch)
    }

    pub fn refresh(&self) -> Result<()> {
        let info: EpochInfo = self
            .rpc("getEpochInfo", serde_json::json!([]))
            .context("getEpochInfo")?;
        let first_slot = info.absolute_slot - info.slot_index;

        // Ask for the schedule at this exact slot, not "current": behind a load
        // balancer the two calls can straddle an epoch boundary and pair a schedule
        // with the wrong first_slot, making every signature look bad.
        let raw: HashMap<String, Vec<u64>> = self
            .rpc("getLeaderSchedule", serde_json::json!([info.absolute_slot]))
            .context("getLeaderSchedule")?;

        let mut leaders = vec![None; info.slots_in_epoch as usize];
        let mut placed = 0usize;
        for (pk, idxs) in raw {
            let pubkey: Pubkey = pk
                .parse()
                .map_err(|_| anyhow!("bad pubkey in leader schedule: {pk}"))?;
            for i in idxs {
                if let Some(slot) = leaders.get_mut(i as usize) {
                    *slot = Some(pubkey);
                    placed += 1;
                }
            }
        }
        if placed == 0 {
            return Err(anyhow!("leader schedule came back empty"));
        }
        // A gap means the schedule and epoch info disagree, i.e. likely the wrong epoch.
        if placed != info.slots_in_epoch as usize {
            return Err(anyhow!(
                "leader schedule covers {placed} of {} slots in epoch {} — the schedule and the \
                 epoch info disagree; refusing to verify against a schedule that may be for a \
                 different epoch",
                info.slots_in_epoch,
                info.epoch
            ));
        }

        *self.inner.write().unwrap() = Some(Epoch {
            epoch: info.epoch,
            first_slot,
            leaders,
        });
        eprintln!(
            "leader schedule: epoch {} first_slot {} ({placed} slots assigned)",
            info.epoch, first_slot
        );
        Ok(())
    }

    fn rpc<T: for<'de> Deserialize<'de>>(&self, method: &str, params: serde_json::Value) -> Result<T> {
        self.rpc
            .call(method, params, std::time::Duration::from_secs(30))
            .map_err(|e| anyhow!("rpc {method}: {e:#}"))
    }
}
