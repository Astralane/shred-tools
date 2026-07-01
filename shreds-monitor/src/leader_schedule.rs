use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const LEADER_SCHEDULE_TABLE: &str = "leader_schedule";

#[derive(Deserialize, Debug, Clone)]
pub struct LeaderScheduleConfig {
    #[serde(default = "default_window_back_secs")]
    pub window_back_secs: u64,
    #[serde(default = "default_window_forward_secs")]
    pub window_forward_secs: u64,
    #[serde(default = "default_tick_secs")]
    pub tick_secs: u64,
    #[serde(default = "default_slot_ms")]
    pub slot_ms: u64,
    #[serde(default = "default_query_timeout_secs")]
    pub query_timeout_secs: u64,
}

impl Default for LeaderScheduleConfig {
    fn default() -> Self {
        Self {
            window_back_secs: default_window_back_secs(),
            window_forward_secs: default_window_forward_secs(),
            tick_secs: default_tick_secs(),
            slot_ms: default_slot_ms(),
            query_timeout_secs: default_query_timeout_secs(),
        }
    }
}

fn default_window_back_secs() -> u64 {
    120
}
fn default_window_forward_secs() -> u64 {
    600
}
fn default_tick_secs() -> u64 {
    60
}
fn default_slot_ms() -> u64 {
    400
}
fn default_query_timeout_secs() -> u64 {
    10
}

pub struct LeaderSchedule {
    window: ArcSwap<HashMap<u64, Pubkey>>,
}

impl LeaderSchedule {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            window: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    pub fn leader(&self, slot: u64) -> Option<Pubkey> {
        self.window.load().get(&slot).copied()
    }

    fn store(&self, window: HashMap<u64, Pubkey>) {
        self.window.store(Arc::new(window));
    }
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct LeaderRow {
    slot: u64,
    leader_pubkey: String,
}

pub async fn run(
    cfg: LeaderScheduleConfig,
    client: clickhouse::Client,
    schedule: Arc<LeaderSchedule>,
    current_slot: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let slot_ms = cfg.slot_ms.max(1);
    let back_slots = cfg.window_back_secs.saturating_mul(1000) / slot_ms;
    let forward_slots = cfg.window_forward_secs.saturating_mul(1000) / slot_ms;
    let tick = Duration::from_secs(cfg.tick_secs.max(1));
    let timeout = Duration::from_secs(cfg.query_timeout_secs.max(1));

    info!(
        table = LEADER_SCHEDULE_TABLE,
        back_slots,
        forward_slots,
        tick_secs = cfg.tick_secs,
        "leader_schedule: service starting"
    );

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let anchor = current_slot.load(Ordering::Relaxed);
        if anchor != 0 {
            let lower = anchor.saturating_sub(back_slots);
            let upper = anchor.saturating_add(forward_slots);
            match tokio::time::timeout(timeout, query_range(&client, lower, upper)).await {
                Ok(Ok(rows)) => {
                    let mut next: HashMap<u64, Pubkey> = HashMap::with_capacity(rows.len());
                    let mut invalid = 0usize;
                    for row in rows {
                        match Pubkey::from_str(row.leader_pubkey.trim()) {
                            Ok(pk) => {
                                next.insert(row.slot, pk);
                            }
                            Err(_) => invalid += 1,
                        }
                    }
                    let resident = next.len();
                    let future = next.keys().filter(|slot| **slot > anchor).count();
                    schedule.store(next);
                    debug!(
                        anchor,
                        lower,
                        upper,
                        resident,
                        future,
                        invalid,
                        "leader_schedule: window refreshed"
                    );
                }
                Ok(Err(e)) => warn!(error = %e, lower, upper, "leader_schedule: query failed"),
                Err(_) => warn!(
                    lower,
                    upper,
                    timeout_secs = cfg.query_timeout_secs,
                    "leader_schedule: query timed out"
                ),
            }
        }

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
    }
    info!("leader_schedule: service exited");
}

async fn query_range(
    client: &clickhouse::Client,
    from: u64,
    to: u64,
) -> clickhouse::error::Result<Vec<LeaderRow>> {
    client
        .query(&format!(
            "SELECT slot, leader_pubkey FROM {LEADER_SCHEDULE_TABLE} WHERE slot >= ? AND slot <= ?"
        ))
        .bind(from)
        .bind(to)
        .fetch_all::<LeaderRow>()
        .await
}
