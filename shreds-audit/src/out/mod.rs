mod archive;
mod manifest;
mod pg;

use std::{path::PathBuf, time::Duration};

use anyhow::Result;

use crate::{
    agg::SetRow,
    config::{Config, ExportMode},
    registry::Registry,
    sigreg::{SourceKind, TxnRow},
    verify::VerifiedShred,
};

pub use archive::ZipSink;
pub use manifest::{
    build_manifest, Counters, Manifest, ProviderPing, TxnCompareSummary, TxnSource, VERSION,
};
pub use pg::{connection_url, PgSink};

#[allow(clippy::large_enum_variant)]
pub enum Sink {
    Zip(ZipSink),
    Pg(PgSink),
    Off,
}

impl Sink {
    pub fn open(
        cfg: &Config,
        mode: ExportMode,
        registry: &Registry,
        dump_shreds: bool,
        dump_txns: bool,
    ) -> Result<Self> {
        Ok(match mode {
            ExportMode::Zip => Sink::Zip(ZipSink::open(cfg, dump_shreds, dump_txns)?),
            ExportMode::Postgres => Sink::Pg(PgSink::open(cfg, registry)?),
            ExportMode::Off => Sink::Off,
        })
    }

    pub fn rotate_interval(&self) -> Option<Duration> {
        match self {
            Sink::Zip(z) => z.rotate_interval(),
            Sink::Pg(_) | Sink::Off => None,
        }
    }

    pub fn flush_interval(&self) -> Option<Duration> {
        match self {
            Sink::Pg(p) => Some(p.flush_interval()),
            Sink::Zip(_) | Sink::Off => None,
        }
    }

    pub fn write_sets(&mut self, reg: &Registry, rows: &[SetRow]) -> Result<u64> {
        match self {
            Sink::Zip(z) => z.write_sets(reg, rows),
            Sink::Pg(p) => {
                p.write_sets(rows);
                Ok(0)
            }
            Sink::Off => Ok(0),
        }
    }

    pub fn write_shreds(&mut self, reg: &Registry, rows: &[VerifiedShred]) -> Result<u64> {
        match self {
            Sink::Zip(z) => z.write_shreds(reg, rows),
            Sink::Pg(_) | Sink::Off => Ok(0),
        }
    }

    pub fn write_txns(&mut self, labels: &[(String, SourceKind)], rows: &[TxnRow]) -> Result<u64> {
        match self {
            Sink::Zip(z) => z.write_txns(labels, rows),
            Sink::Pg(_) | Sink::Off => Ok(0),
        }
    }

    pub fn rotate(&mut self, manifest: Manifest) -> Result<Option<PathBuf>> {
        match self {
            Sink::Zip(z) => z.rotate(manifest).map(Some),
            Sink::Pg(_) | Sink::Off => Ok(None),
        }
    }

    pub fn flush(&mut self, manifest: &Manifest) -> Result<()> {
        if let Sink::Pg(p) = self {
            p.flush(manifest);
        }
        Ok(())
    }

    pub fn write_live(
        &mut self,
        reg: &Registry,
        rows: &[SetRow],
        manifest: Manifest,
    ) -> Result<()> {
        match self {
            Sink::Zip(z) => z.write_live(reg, rows, manifest),
            Sink::Pg(_) | Sink::Off => Ok(()),
        }
    }

    pub fn close(self, manifest: Manifest) -> Result<Option<PathBuf>> {
        match self {
            Sink::Zip(z) => z.close(manifest).map(Some),
            Sink::Pg(p) => {
                p.close();
                Ok(None)
            }
            Sink::Off => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct WindowStats {
    pub rows_sets: u64,
    pub rows_shreds: u64,
    pub rows_txns: u64,
    pub min_slot: u64,
    pub max_slot: u64,
    pub invalid_sig: u64,
    pub invalid_data: u64,
    pub invalid_unknown: u64,
}

impl Default for WindowStats {
    fn default() -> Self {
        Self {
            rows_sets: 0,
            rows_shreds: 0,
            rows_txns: 0,
            min_slot: u64::MAX,
            max_slot: 0,
            invalid_sig: 0,
            invalid_data: 0,
            invalid_unknown: 0,
        }
    }
}

impl WindowStats {
    pub fn observe_sets(&mut self, rows: &[SetRow]) {
        for r in rows {
            self.min_slot = self.min_slot.min(r.slot);
            self.max_slot = self.max_slot.max(r.slot);
            self.invalid_sig += r.invalid_sig as u64;
            self.invalid_data += r.invalid_data as u64;
            self.invalid_unknown += r.invalid_unknown as u64;
        }
    }

    pub fn from_sets(rows: &[SetRow]) -> Self {
        let mut s = Self::default();
        s.observe_sets(rows);
        s.rows_sets = rows.len() as u64;
        s
    }

    pub fn min_slot_or_zero(&self) -> u64 {
        if self.min_slot == u64::MAX {
            0
        } else {
            self.min_slot
        }
    }
}

pub fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}

pub fn now_unix_ns() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    d.as_secs() as i64 * 1_000_000_000 + d.subsec_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_row(slot: u64, invalid_sig: u32, invalid_data: u32, invalid_unknown: u32) -> SetRow {
        SetRow {
            provider: 0,
            slot,
            fec_set_index: 0,
            leader: None,
            first_ns: 0,
            decode_ns: None,
            last_ns: 0,
            n_data: 0,
            n_code: 0,
            expected_total: None,
            missed: 0,
            invalid: invalid_sig + invalid_data + invalid_unknown,
            invalid_sig,
            invalid_data,
            invalid_unknown,
            duplicated: 0,
            sig_unverifiable: 0,
            is_valid: true,
            last_in_slot: false,
        }
    }

    #[test]
    fn window_stats_fold_min_max_and_the_invalid_split() {
        let mut s = WindowStats::default();
        assert_eq!(s.min_slot_or_zero(), 0, "an untouched window reports slot 0");

        s.observe_sets(&[set_row(1_010, 1, 0, 0), set_row(1_005, 0, 2, 0)]);
        s.observe_sets(&[set_row(1_020, 0, 0, 3)]);

        assert_eq!(s.min_slot, 1_005);
        assert_eq!(s.min_slot_or_zero(), 1_005);
        assert_eq!(s.max_slot, 1_020);
        assert_eq!(s.invalid_sig, 1);
        assert_eq!(s.invalid_data, 2);
        assert_eq!(s.invalid_unknown, 3);
        assert_eq!(s.rows_sets, 0, "row counts come back from the sink, not the fold");
    }

    #[test]
    fn window_stats_from_sets_counts_only_sets() {
        let s = WindowStats::from_sets(&[set_row(7, 0, 0, 0), set_row(9, 0, 0, 0)]);
        assert_eq!(s.rows_sets, 2);
        assert_eq!(s.rows_shreds, 0);
        assert_eq!(s.rows_txns, 0);
        assert_eq!(s.min_slot, 7);
        assert_eq!(s.max_slot, 9);
    }
}
