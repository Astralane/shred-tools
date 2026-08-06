//! Archive writer: Parquet + manifest, zipped.
//!
//! `fec_sets.parquet` is the primary artifact — one row per
//! (provider, slot, fec_set_index) with absolute nanosecond arrival times.
//! Per-shred rows are optional (`dump_shreds`): at ~100 kpps a ten-minute
//! capture is ~60 M rows / ~1 GB.

use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrow_array::{
    builder::{BooleanBuilder, Int64Builder, StringBuilder, UInt32Builder, UInt64Builder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::Serialize;

use crate::{agg::SetRow, registry::Registry, verify::VerifiedShred};

#[derive(Serialize, Clone)]
pub struct Manifest {
    pub tool: &'static str,
    pub tool_version: &'static str,
    pub schema_version: u32,
    pub hostname: String,
    pub started_at_unix_ns: i64,
    pub ended_at_unix_ns: i64,
    pub clock_source: &'static str,
    pub timestamp_semantics: &'static str,
    pub providers: Vec<String>,
    pub rpc_url: String,
    pub leader_schedule_epoch: Option<u64>,
    pub min_slot: u64,
    pub max_slot: u64,
    pub rows_fec_sets: u64,
    pub rows_shreds: u64,
    pub counters: Counters,
    /// One row per (provider, source IP) with the latest ping RTT. For an
    /// IP-configured provider these are the configured IPs; for a port-configured
    /// provider they are the source IPs observed sending to it.
    pub provider_pings: Vec<ProviderPing>,
    /// Leader identity pubkey (base58) -> display name, from on-chain
    /// validator-info. Only leaders that published a name appear; the viewer
    /// falls back to the pubkey for the rest. Empty if names couldn't be fetched.
    pub leader_names: std::collections::HashMap<String, String>,
    /// shred-vs-gRPC transaction-timing comparison, present only when the config
    /// declared `grpc_sources`. Null otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_compare: Option<TxnCompareSummary>,
    /// DoubleZero Edge multicast membership, present only when the config
    /// declared a `multicast` block. Read this before comparing a multicast
    /// provider against anything: a group that was not joined for the whole
    /// window did not lose races, it was not in them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multicast: Option<MulticastStatus>,
    pub notes: Vec<String>,
}

/// Multicast membership over the capture.
#[derive(Serialize, Clone, Default)]
pub struct MulticastStatus {
    pub groups: Vec<MulticastGroupStatus>,
    /// Membership transitions inside this archive's window, oldest first.
    pub events: Vec<MulticastEvent>,
    /// Transitions discarded after the retention cap. Nonzero means the tunnel
    /// flapped more than the event list can hold; the per-group counters are
    /// still exact.
    pub events_dropped: u64,
}

/// One group's membership state and history.
#[derive(Serialize, Clone)]
pub struct MulticastGroupStatus {
    pub group: String,
    pub port: u16,
    /// Interface address the join was made on. `0.0.0.0` means the kernel
    /// routing table chose it.
    pub interface: String,
    pub require_route: bool,
    /// Joined as of when this manifest was written.
    pub joined: bool,
    /// Nanoseconds this group was joined during **this archive's window**, not
    /// the whole run. Compare it against the window length: anything well short
    /// of it means the provider on this port was absent for part of the capture,
    /// and its `missed` and coverage are wrong by that much.
    pub joined_ns: i64,
    pub joins: u64,
    pub leaves: u64,
    /// `IP_ADD_MEMBERSHIP` failures. A group with errors and no joins never
    /// delivered a single shred.
    pub join_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// First successful join, or null if it never joined.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_joined_at_unix_ns: Option<i64>,
}

/// A single membership transition.
#[derive(Serialize, Clone)]
pub struct MulticastEvent {
    pub at_unix_ns: i64,
    pub group: String,
    /// "join", "leave", or "join_failed".
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Transaction-timing comparison across sources. The reconstructed shred stream
/// and every gRPC feed are peers here — each is one row, framed like a provider
/// in the FEC-set comparison: winrate (delivered first) and µs behind the fastest.
#[derive(Serialize, Clone, Default)]
pub struct TxnCompareSummary {
    /// Distinct transaction signatures seen across all sources.
    pub distinct_signatures: u64,
    /// Transactions seen by at least two sources (the ones a race is defined on).
    pub contested: u64,
    pub sources: Vec<TxnSource>,
    /// Slots sampled and compared against the block onchain. Zero when the
    /// onchain audit is off — every `onchain_*` field below is then meaningless.
    pub onchain_slots_checked: u64,
    /// Sampled slots the cluster produced no block for (a skipped leader).
    /// Expected, and not a fault of any source.
    pub onchain_slots_unavailable: u64,
    /// `getBlock` calls that failed. Nonzero means the audit sampled less than
    /// the elapsed time suggests.
    pub onchain_rpc_errors: u64,
    /// Most recent `getBlock` failure, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onchain_last_error: Option<String>,
}

/// One source in the transaction-timing comparison — the shred stream or a gRPC
/// feed, presented identically.
#[derive(Serialize, Clone, Default)]
pub struct TxnSource {
    pub name: String,
    /// "shreds" for the reconstructed shred stream, "grpc" for a gRPC feed.
    pub kind: String,
    /// Distinct transactions this source delivered.
    pub seen: u64,
    /// Contested transactions this source was present for.
    pub contested: u64,
    /// Fraction of its contested transactions this source delivered first.
    pub winrate: Option<f64>,
    /// Microseconds behind the earliest source, over its contested transactions.
    pub behind_mean_us: Option<f64>,
    pub behind_p50_us: Option<f64>,
    pub behind_p90_us: Option<f64>,
    pub behind_p99_us: Option<f64>,

    // ---- onchain audit (see verification::onchain_signatures) ----
    // A sampled audit against `getBlock`, not a full ledger: the counts below
    // are over the sampled slots only, so `onchain_bad_pct` is the number to
    // compare sources on, not `onchain_bad`.
    /// Sampled slots this source delivered something for, and was scored on.
    pub onchain_slots_checked: u64,
    /// Sampled slots this source delivered nothing at all for. Not scored — a
    /// disconnected feed is not a feed that corrupted a block.
    pub onchain_slots_absent: u64,
    /// Transactions in the blocks this source was scored against — the
    /// denominator of `onchain_bad_pct`.
    pub onchain_txns: u64,
    /// Onchain transactions this source never delivered.
    pub onchain_missed: u64,
    /// Transactions this source delivered that the block does not contain.
    pub onchain_corrupted: u64,
    /// Transactions this source delivered more than once for the same slot.
    pub onchain_duplicated: u64,
    /// `missed + corrupted + duplicated` — any discrepancy with the block.
    pub onchain_bad: u64,
    /// `onchain_bad / onchain_txns`. Null until the source has been scored.
    pub onchain_bad_pct: Option<f64>,
}

/// A single pinged source IP belonging to a provider. RTT is a coarse
/// reachability/network-distance hint — NOT comparable to the decode deltas.
#[derive(Serialize, Clone)]
pub struct ProviderPing {
    pub provider: String,
    pub ip: String,
    /// "configured" (declared in the config) or "observed" (seen on the wire).
    pub source: &'static str,
    /// Average ICMP RTT in ms, or null if the host did not reply / not yet pinged.
    pub rtt_ms: Option<f64>,
    /// When this IP was last pinged (unix ns), or null if never.
    pub checked_at_unix_ns: Option<i64>,
}

#[derive(Serialize, Clone, Default)]
pub struct Counters {
    pub udp_received: u64,
    pub udp_unmatched: u64,
    pub udp_no_timestamp: u64,
    pub udp_channel_full: u64,
    /// Datagrams the kernel dropped from our socket queue (SO_RXQ_OVFL). OUR
    /// loss, not the provider's — but they are missing from `missed` all the same.
    pub udp_kernel_dropped: u64,
    /// Datagrams too large for the receive buffer; truncated by the kernel and
    /// discarded rather than parsed into a false "invalid shred".
    pub udp_truncated: u64,
    pub shreds_parsed: u64,
    pub shreds_malformed: u64,
    /// Shreds whose variant this build does not understand. Never invalid.
    pub shreds_unsupported_variant: u64,
    /// Valid, signature-checked Solana pings that shared the socket with the
    /// shreds. Not shreds, and not a defect — see `verify::is_ping`.
    pub non_shred_pings: u64,
    pub shreds_wrong_version: u64,
    pub shreds_no_merkle_root: u64,
    pub shreds_no_leader: u64,
    /// Shreds that failed verification. `shreds_sig_bad == invalid_sig +
    /// invalid_data + invalid_unknown`, which is the split that matters:
    /// `invalid_sig` is a broken proof over the leader's real data, while
    /// `invalid_data` is content the leader never signed.
    pub shreds_sig_bad: u64,
    pub invalid_sig: u64,
    pub invalid_data: u64,
    pub invalid_unknown: u64,
    pub ed25519_verifies: u64,
    pub batch_fallbacks: u64,
    /// Shreds dropped because their FEC set had already been finalized and
    /// written (slot at or below the window floor). A late straggler or
    /// retransmit from a slow/reordering provider; counted so it is visible,
    /// never silently folded into a set it can no longer belong to.
    pub shreds_after_window: u64,
}

fn sets_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("fec_set_index", DataType::UInt32, false),
        Field::new("leader", DataType::Utf8, true),
        Field::new("first_ns", DataType::Int64, false),
        Field::new("decode_ns", DataType::Int64, true),
        Field::new("last_ns", DataType::Int64, false),
        Field::new("n_data", DataType::UInt32, false),
        Field::new("n_code", DataType::UInt32, false),
        Field::new("expected_total", DataType::UInt32, true),
        Field::new("missed", DataType::UInt32, false),
        Field::new("invalid", DataType::UInt32, false),
        Field::new("invalid_sig", DataType::UInt32, false),
        Field::new("invalid_data", DataType::UInt32, false),
        Field::new("invalid_unknown", DataType::UInt32, false),
        Field::new("duplicated", DataType::UInt32, false),
        Field::new("sig_unverifiable", DataType::UInt32, false),
        Field::new("is_valid", DataType::Boolean, false),
        Field::new("last_in_slot", DataType::Boolean, false),
    ]))
}

fn shreds_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("fec_set_index", DataType::UInt32, false),
        Field::new("shred_index", DataType::UInt32, false),
        Field::new("is_code", DataType::Boolean, false),
        Field::new("rx_unix_ns", DataType::Int64, false),
        Field::new("sig_ok", DataType::Boolean, true),
        Field::new("merkle_ok", DataType::Boolean, false),
        Field::new("leader", DataType::Utf8, true),
        // SHA-256 of the signed leaf (headers + block data, excluding the merkle
        // proof). Lets the bad-signature / bad-data split be re-derived offline:
        // two copies of one shred with equal data_hash carry identical data.
        Field::new("data_hash", DataType::Utf8, true),
    ]))
}

fn hex32(h: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in h {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build()
}

pub struct Archive {
    dir: PathBuf,
    sets_path: PathBuf,
    shreds_path: Option<PathBuf>,
    sets_w: ArrowWriter<File>,
    shreds_w: Option<ArrowWriter<File>>,
    pub rows_sets: u64,
    pub rows_shreds: u64,
    pub min_slot: u64,
    pub max_slot: u64,
    /// Per-archive totals of the invalid split, summed from the rows written.
    /// They live here, not in `VerifyStats`, because the classification is only
    /// settled when a set is finalized — not when a shred is verified.
    pub invalid_sig: u64,
    pub invalid_data: u64,
    pub invalid_unknown: u64,
}

impl Archive {
    pub fn create(work_dir: &Path, dump_shreds: bool) -> Result<Self> {
        fs::create_dir_all(work_dir)?;
        let sets_path = work_dir.join("fec_sets.parquet");
        let sets_w = ArrowWriter::try_new(File::create(&sets_path)?, sets_schema(), Some(props()))?;

        let (shreds_path, shreds_w) = if dump_shreds {
            let p = work_dir.join("shreds.parquet");
            let w = ArrowWriter::try_new(File::create(&p)?, shreds_schema(), Some(props()))?;
            (Some(p), Some(w))
        } else {
            (None, None)
        };

        Ok(Self {
            dir: work_dir.to_path_buf(),
            sets_path,
            shreds_path,
            sets_w,
            shreds_w,
            invalid_sig: 0,
            invalid_data: 0,
            invalid_unknown: 0,
            rows_sets: 0,
            rows_shreds: 0,
            min_slot: u64::MAX,
            max_slot: 0,
        })
    }

    pub fn write_sets(&mut self, reg: &Registry, rows: &[SetRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut provider = StringBuilder::new();
        let mut slot = UInt64Builder::new();
        let mut fec = UInt32Builder::new();
        let mut leader = StringBuilder::new();
        let mut first = Int64Builder::new();
        let mut decode = Int64Builder::new();
        let mut last = Int64Builder::new();
        let mut n_data = UInt32Builder::new();
        let mut n_code = UInt32Builder::new();
        let mut expected = UInt32Builder::new();
        let mut missed = UInt32Builder::new();
        let mut invalid = UInt32Builder::new();
        let mut inv_sig = UInt32Builder::new();
        let mut inv_data = UInt32Builder::new();
        let mut inv_unknown = UInt32Builder::new();
        let mut dup = UInt32Builder::new();
        let mut unver = UInt32Builder::new();
        let mut valid = BooleanBuilder::new();
        let mut lis = BooleanBuilder::new();

        for r in rows {
            self.min_slot = self.min_slot.min(r.slot);
            self.max_slot = self.max_slot.max(r.slot);
            provider.append_value(reg.name(r.provider));
            slot.append_value(r.slot);
            fec.append_value(r.fec_set_index);
            match &r.leader {
                Some(pk) => leader.append_value(pk.to_string()),
                None => leader.append_null(),
            }
            first.append_value(r.first_ns);
            match r.decode_ns {
                Some(v) => decode.append_value(v),
                None => decode.append_null(),
            }
            last.append_value(r.last_ns);
            n_data.append_value(r.n_data);
            n_code.append_value(r.n_code);
            match r.expected_total {
                Some(v) => expected.append_value(v),
                None => expected.append_null(),
            }
            missed.append_value(r.missed);
            invalid.append_value(r.invalid);
            inv_sig.append_value(r.invalid_sig);
            inv_data.append_value(r.invalid_data);
            inv_unknown.append_value(r.invalid_unknown);
            self.invalid_sig += r.invalid_sig as u64;
            self.invalid_data += r.invalid_data as u64;
            self.invalid_unknown += r.invalid_unknown as u64;
            dup.append_value(r.duplicated);
            unver.append_value(r.sig_unverifiable);
            valid.append_value(r.is_valid);
            lis.append_value(r.last_in_slot);
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(provider.finish()),
            Arc::new(slot.finish()),
            Arc::new(fec.finish()),
            Arc::new(leader.finish()),
            Arc::new(first.finish()),
            Arc::new(decode.finish()),
            Arc::new(last.finish()),
            Arc::new(n_data.finish()),
            Arc::new(n_code.finish()),
            Arc::new(expected.finish()),
            Arc::new(missed.finish()),
            Arc::new(invalid.finish()),
            Arc::new(inv_sig.finish()),
            Arc::new(inv_data.finish()),
            Arc::new(inv_unknown.finish()),
            Arc::new(dup.finish()),
            Arc::new(unver.finish()),
            Arc::new(valid.finish()),
            Arc::new(lis.finish()),
        ];
        let batch = RecordBatch::try_new(sets_schema(), cols)?;
        self.sets_w.write(&batch)?;
        self.rows_sets += rows.len() as u64;
        Ok(())
    }

    pub fn write_shreds(&mut self, reg: &Registry, rows: &[VerifiedShred]) -> Result<()> {
        let Some(w) = self.shreds_w.as_mut() else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        let mut provider = StringBuilder::new();
        let mut slot = UInt64Builder::new();
        let mut fec = UInt32Builder::new();
        let mut idx = UInt32Builder::new();
        let mut is_code = BooleanBuilder::new();
        let mut rx = Int64Builder::new();
        let mut sig_ok = BooleanBuilder::new();
        let mut merkle_ok = BooleanBuilder::new();
        let mut leader = StringBuilder::new();
        let mut leaf = StringBuilder::new();

        for r in rows {
            provider.append_value(reg.name(r.provider));
            slot.append_value(r.slot);
            fec.append_value(r.fec_set_index);
            idx.append_value(r.shred_index);
            is_code.append_value(r.is_code);
            rx.append_value(r.rx_unix_ns);
            match r.sig_ok {
                Some(v) => sig_ok.append_value(v),
                None => sig_ok.append_null(),
            }
            merkle_ok.append_value(r.merkle_ok);
            match &r.leader {
                Some(pk) => leader.append_value(pk.to_string()),
                None => leader.append_null(),
            }
            match &r.data_hash {
                Some(h) => leaf.append_value(hex32(h)),
                None => leaf.append_null(),
            }
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(provider.finish()),
            Arc::new(slot.finish()),
            Arc::new(fec.finish()),
            Arc::new(idx.finish()),
            Arc::new(is_code.finish()),
            Arc::new(rx.finish()),
            Arc::new(sig_ok.finish()),
            Arc::new(merkle_ok.finish()),
            Arc::new(leader.finish()),
            Arc::new(leaf.finish()),
        ];
        let batch = RecordBatch::try_new(shreds_schema(), cols)?;
        w.write(&batch)?;
        self.rows_shreds += rows.len() as u64;
        Ok(())
    }

    /// Close the writers, drop the manifest next to them, zip the directory.
    pub fn finish(mut self, out_zip: &Path, manifest: Manifest) -> Result<PathBuf> {
        self.sets_w.close().context("closing fec_sets.parquet")?;
        if let Some(w) = self.shreds_w.take() {
            w.close().context("closing shreds.parquet")?;
        }

        let manifest_path = self.dir.join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        let file = File::create(out_zip)
            .with_context(|| format!("creating {}", out_zip.display()))?;
        let mut zip = zip::ZipWriter::new(BufWriter::new(file));
        let opts: zip::write::FileOptions<()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        let mut members = vec![manifest_path.clone(), self.sets_path.clone()];
        if let Some(p) = &self.shreds_path {
            members.push(p.clone());
        }
        for path in &members {
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            zip.start_file(name, opts)?;
            let bytes = fs::read(path)?;
            zip.write_all(&bytes)?;
        }
        zip.finish()?;

        for path in &members {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir(&self.dir);
        Ok(out_zip.to_path_buf())
    }
}

pub fn now_unix_ns() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    d.as_secs() as i64 * 1_000_000_000 + d.subsec_nanos() as i64
}
