pub mod audit;
pub mod bundles;
pub mod check;
pub mod db;
pub mod spec;

use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, GrpcMode};
use crate::sigreg::SigRegistry;
use audit::FilterAudit;
use bundles::Bundle;
use db::DbHandle;

pub struct Rotation {
    bundles: Vec<Arc<Bundle>>,
    bag: Vec<usize>,
    min_secs: u64,
    max_secs: u64,
    rng: u64,
}

impl Rotation {
    pub fn new(bundles: Vec<Arc<Bundle>>, min_secs: u64, max_secs: u64, seed: u64) -> Self {
        Self {
            bundles,
            bag: Vec::new(),
            min_secs,
            max_secs,
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    pub fn next(&mut self) -> (Arc<Bundle>, Duration) {
        if self.bag.is_empty() {
            self.bag = (0..self.bundles.len()).collect();
            for i in (1..self.bag.len()).rev() {
                let j = (self.next_u64() % (i as u64 + 1)) as usize;
                self.bag.swap(i, j);
            }
        }
        let idx = self.bag.pop().expect("refilled above");
        let span = self.max_secs - self.min_secs + 1;
        let secs = self.min_secs + self.next_u64() % span;
        (self.bundles[idx].clone(), Duration::from_secs(secs))
    }
}

pub struct FilterAuditor {
    pub audit: Arc<FilterAudit>,
    checker: JoinHandle<()>,
    db_writer: Option<JoinHandle<()>>,
    db: DbHandle,
    cancel: CancellationToken,
}

impl FilterAuditor {
    pub fn start(
        cfg: &Config,
        reg: Arc<Mutex<SigRegistry>>,
        db_url: Option<String>,
    ) -> Result<Option<Self>> {
        if !cfg.grpc_sources.iter().any(|g| cfg.rotating(g)) {
            return Ok(None);
        }
        let rpc = cfg.onchain_rpc_endpoint()?;
        let (db, db_writer) = match db_url {
            Some(url) => {
                let (db, writer) = db::spawn_writer(url)?;
                (db, Some(writer))
            }
            None => (DbHandle::disabled(), None),
        };
        let fr = &cfg.filter_rotation;
        let audit = FilterAudit::new(fr.sample_every_slots, reg.clone());
        eprintln!(
            "filter-audit: rotating {} gRPC source(s) through {} bundle(s), {}-{}s per window; \
             checking every {}th slot against getBlock via {}{}",
            cfg.grpc_sources.iter().filter(|g| cfg.rotating(g)).count(),
            fr.effective_bundles().len(),
            fr.min_secs,
            fr.max_secs,
            fr.sample_every_slots,
            rpc.label(),
            if db_writer.is_some() {
                ""
            } else {
                " (no postgres export: results go to stderr only)"
            },
        );
        let cancel = CancellationToken::new();
        let checker = check::spawn(
            audit.clone(),
            reg,
            rpc,
            cfg.onchain_lag_slots,
            db.clone(),
            cancel.clone(),
        )?;
        Ok(Some(Self {
            audit,
            checker,
            db_writer,
            db,
            cancel,
        }))
    }

    pub fn rotation(cfg: &Config, mode: GrpcMode, seed: u64) -> Rotation {
        let fr = &cfg.filter_rotation;
        let bundles = bundles::for_mode(&fr.effective_bundles(), mode == GrpcMode::Deshred);
        Rotation::new(bundles, fr.min_secs, fr.max_secs, seed)
    }

    pub fn finish(self) {
        self.cancel.cancel();
        let _ = self.checker.join();
        let dropped = self.db.dropped();
        // The writer drains until every sender, including this one, is gone.
        drop(self.db);
        if let Some(w) = self.db_writer {
            let _ = w.join();
        }
        if dropped > 0 {
            eprintln!(
                "filter-audit: {dropped} row(s) dropped because the database writer fell behind"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_bundle_runs_once_per_cycle_within_the_duration_bounds() {
        let bundles = bundles::for_mode(&bundles::defaults(), false);
        let n = bundles.len();
        let mut r = Rotation::new(bundles, 30, 60, 7);
        for _ in 0..3 {
            let mut seen: Vec<String> = (0..n)
                .map(|_| {
                    let (b, d) = r.next();
                    assert!((30..=60).contains(&d.as_secs()));
                    b.name.clone()
                })
                .collect();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), n, "a full cycle covers every bundle");
        }
    }
}
