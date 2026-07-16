//! Leader schedule, fetched over JSON-RPC and cached per epoch.
//!
//! Without this we cannot verify a signature at all: the signature on a shred
//! is made by the slot's leader, so `slot -> leader pubkey` is the only thing
//! that turns "this is a well-formed shred" into "this is a shred the leader
//! actually signed".

use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

#[derive(Deserialize)]
struct RpcResp<T> {
    result: Option<T>,
    error: Option<serde_json::Value>,
}

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

/// Mainnet epoch length. Only used as a floor for the plausibility window, so a
/// short (test) schedule still admits the slots around it.
const SLOTS_PER_EPOCH: u64 = 432_000;

/// What the schedule can say about a slot.
pub enum SlotVerdict {
    /// Far enough outside the loaded schedule that the datagram is not a shred.
    Implausible,
    /// The slot's leader.
    Leader(Pubkey),
    /// A plausible slot the schedule has no entry for. Callers must treat this
    /// as "cannot verify", never as "invalid".
    Unknown,
}

struct Epoch {
    epoch: u64,
    first_slot: u64,
    /// One entry per slot in the epoch. `None` where the RPC gave us nothing.
    leaders: Vec<Option<Pubkey>>,
}

pub struct LeaderSchedule {
    rpc_url: String,
    inner: RwLock<Option<Epoch>>,
}

impl LeaderSchedule {
    pub fn new(rpc_url: &str) -> Arc<Self> {
        Arc::new(Self {
            rpc_url: rpc_url.to_string(),
            inner: RwLock::new(None),
        })
    }

    /// Build a schedule with a fixed mapping, bypassing RPC. Test-only.
    #[cfg(test)]
    pub fn for_test(first_slot: u64, leaders: Vec<Option<Pubkey>>) -> Arc<Self> {
        Arc::new(Self {
            rpc_url: String::new(),
            inner: RwLock::new(Some(Epoch {
                epoch: 0,
                first_slot,
                leaders,
            })),
        })
    }

    /// What we can say about `slot`, in one lock acquisition.
    pub fn classify(&self, slot: u64) -> SlotVerdict {
        let Ok(guard) = self.inner.read() else {
            return SlotVerdict::Unknown;
        };
        let Some(e) = guard.as_ref() else {
            return SlotVerdict::Unknown;
        };

        // A slot an epoch or more outside the schedule we hold is not a shred we
        // have the wrong leader for — it is not a shred. Some datagrams happen to
        // carry the shred kind bits and parse into a nonsense slot; attributing
        // one to a FEC set invents a phantom row and drags the archive's slot
        // range (and the refresh trigger that reads it) off to u64 nonsense.
        // The window is deliberately loose — a full epoch below and two above —
        // so a genuine shred near an epoch boundary is never discarded here. It
        // simply lands in `Unknown` and is reported as unverifiable, as before.
        let span = (e.leaders.len() as u64).max(SLOTS_PER_EPOCH);
        let lo = e.first_slot.saturating_sub(span);
        let hi = e.first_slot.saturating_add(span.saturating_mul(2));
        if slot < lo || slot >= hi {
            return SlotVerdict::Implausible;
        }

        if slot < e.first_slot {
            return SlotVerdict::Unknown;
        }
        let idx = (slot - e.first_slot) as usize;
        match e.leaders.get(idx).copied().flatten() {
            Some(pk) => SlotVerdict::Leader(pk),
            None => SlotVerdict::Unknown,
        }
    }

    /// True when `slot` falls outside the epoch we currently hold.
    pub fn needs_refresh(&self, slot: u64) -> bool {
        match self.inner.read() {
            Ok(g) => match g.as_ref() {
                None => true,
                Some(e) => slot < e.first_slot || slot >= e.first_slot + e.leaders.len() as u64,
            },
            Err(_) => true,
        }
    }

    pub fn epoch(&self) -> Option<u64> {
        self.inner.read().ok()?.as_ref().map(|e| e.epoch)
    }

    pub fn refresh(&self) -> Result<()> {
        let info: EpochInfo = self
            .rpc("getEpochInfo", serde_json::json!([]))
            .context("getEpochInfo")?;
        let first_slot = info.absolute_slot - info.slot_index;

        // Pin the schedule to the slot we just learned about, rather than asking
        // for "the current epoch" a second time.
        //
        // `getEpochInfo` and `getLeaderSchedule` are two separate requests, and a
        // public endpoint is a load balancer: they can land on different backends.
        // Ask for "current" twice across an epoch boundary and one node answers
        // for epoch N while the other answers for N+1 — we would then index a
        // schedule from one epoch with a first_slot from the other, get the wrong
        // leader for *every* slot, and report every shred from every provider as a
        // bad signature. Passing the slot makes both answers refer to the same
        // epoch by construction.
        let raw: std::collections::HashMap<String, Vec<u64>> = self
            .rpc(
                "getLeaderSchedule",
                serde_json::json!([info.absolute_slot]),
            )
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
        // A complete schedule names a leader for every slot of the epoch. If it
        // does not, the schedule and the epoch we sized it against disagree —
        // exactly the mismatch that would silently hand us the wrong leader — so
        // refuse to pretend the gap is a normal schedule gap.
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
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        });
        let resp: RpcResp<T> = ureq::post(&self.rpc_url)
            .set("content-type", "application/json")
            .timeout(std::time::Duration::from_secs(30))
            .send_json(body)?
            .into_json()?;
        if let Some(err) = resp.error {
            return Err(anyhow!("rpc {method} error: {err}"));
        }
        resp.result.ok_or_else(|| anyhow!("rpc {method}: empty result"))
    }
}
