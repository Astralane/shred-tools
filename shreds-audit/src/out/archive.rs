use std::{
    fs::{self, File},
    io::{self, BufWriter},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
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

use crate::{
    agg::SetRow,
    config::Config,
    registry::Registry,
    sigreg::{SourceKind, TxnRow},
    verify::VerifiedShred,
};

use super::{now_unix_ns, Manifest};

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
        Field::new("data_hash", DataType::Utf8, true),
    ]))
}

fn txns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("signature", DataType::Utf8, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("source_mode", DataType::Utf8, false),
        Field::new("first_rx_unix_ns", DataType::Int64, false),
        Field::new("server_created_at_ns", DataType::Int64, true),
        Field::new("duplicate_count", DataType::UInt32, false),
        Field::new("is_vote", DataType::Boolean, true),
        Field::new("message_size", DataType::UInt32, true),
        Field::new("connection_id", DataType::UInt32, true),
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

fn parquet_writer(path: &Path, schema: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();
    Ok(ArrowWriter::try_new(File::create(path)?, schema, Some(props))?)
}

pub struct ZipSink {
    out_dir: PathBuf,
    dump_shreds: bool,
    dump_txns: bool,
    rotate_secs: u64,
    archive: Archive,
}

impl ZipSink {
    pub fn open(cfg: &Config, dump_shreds: bool, dump_txns: bool) -> Result<Self> {
        let out_dir = PathBuf::from(&cfg.output_dir);
        fs::create_dir_all(&out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
        let archive = Archive::create(&work_dir(&out_dir), dump_shreds, dump_txns)?;
        Ok(Self {
            out_dir,
            dump_shreds,
            dump_txns,
            rotate_secs: cfg.rotate_secs,
            archive,
        })
    }

    pub fn rotate_interval(&self) -> Option<Duration> {
        (self.rotate_secs > 0).then(|| Duration::from_secs(self.rotate_secs))
    }

    pub fn write_sets(&mut self, reg: &Registry, rows: &[SetRow]) -> Result<u64> {
        self.archive.write_sets(reg, rows)
    }

    pub fn write_shreds(&mut self, reg: &Registry, rows: &[VerifiedShred]) -> Result<u64> {
        self.archive.write_shreds(reg, rows)
    }

    pub fn write_txns(&mut self, labels: &[(String, SourceKind)], rows: &[TxnRow]) -> Result<u64> {
        self.archive.write_txns(labels, rows)
    }

    pub fn rotate(&mut self, manifest: Manifest) -> Result<PathBuf> {
        let next = Archive::create(&work_dir(&self.out_dir), self.dump_shreds, self.dump_txns)?;
        let path = self.out_dir.join(archive_name(&manifest));
        std::mem::replace(&mut self.archive, next).finish(&path, manifest)
    }

    pub fn write_live(&self, reg: &Registry, rows: &[SetRow], mut manifest: Manifest) -> Result<()> {
        let mut a = Archive::create(&self.out_dir.join(".live-work"), false, false)?;
        a.write_sets(reg, rows)?;
        manifest.notes.insert(
            0,
            "LIVE snapshot — an in-progress capture window, refreshed periodically. It is replaced \
             atomically each refresh; the full run lands in the rotated timestamped archives."
                .to_string(),
        );
        let tmp = self.out_dir.join(".live.zip.tmp");
        a.finish(&tmp, manifest)?;
        fs::rename(&tmp, self.out_dir.join("live.zip"))?;
        Ok(())
    }

    pub fn close(self, manifest: Manifest) -> Result<PathBuf> {
        let path = self.out_dir.join(archive_name(&manifest));
        self.archive.finish(&path, manifest)
    }
}

fn archive_name(manifest: &Manifest) -> String {
    format!(
        "shred-audit-{}-{}.zip",
        chrono::DateTime::from_timestamp_nanos(manifest.started_at_unix_ns)
            .format("%Y%m%dT%H%M%SZ"),
        manifest.hostname
    )
}

fn work_dir(out_dir: &Path) -> PathBuf {
    out_dir.join(format!(".work-{}", now_unix_ns()))
}

const SETS_FILE: &str = "fec_sets.parquet";
const SHREDS_FILE: &str = "shreds.parquet";
const TXNS_FILE: &str = "transactions.parquet";

struct Archive {
    dir: PathBuf,
    sets: ArrowWriter<File>,
    shreds: Option<ArrowWriter<File>>,
    txns: Option<ArrowWriter<File>>,
}

impl Archive {
    fn create(dir: &Path, dump_shreds: bool, dump_txns: bool) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let sets = parquet_writer(&dir.join(SETS_FILE), sets_schema())?;
        let shreds = dump_shreds
            .then(|| parquet_writer(&dir.join(SHREDS_FILE), shreds_schema()))
            .transpose()?;
        let txns = dump_txns
            .then(|| parquet_writer(&dir.join(TXNS_FILE), txns_schema()))
            .transpose()?;
        Ok(Self {
            dir: dir.to_path_buf(),
            sets,
            shreds,
            txns,
        })
    }

    fn write_sets(&mut self, reg: &Registry, rows: &[SetRow]) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
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
            provider.append_value(reg.name(r.provider));
            slot.append_value(r.slot);
            fec.append_value(r.fec_set_index);
            leader.append_option(r.leader.map(|pk| pk.to_string()));
            first.append_value(r.first_ns);
            decode.append_option(r.decode_ns);
            last.append_value(r.last_ns);
            n_data.append_value(r.n_data);
            n_code.append_value(r.n_code);
            expected.append_option(r.expected_total);
            missed.append_value(r.missed);
            invalid.append_value(r.invalid);
            inv_sig.append_value(r.invalid_sig);
            inv_data.append_value(r.invalid_data);
            inv_unknown.append_value(r.invalid_unknown);
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
        self.sets.write(&RecordBatch::try_new(sets_schema(), cols)?)?;
        Ok(rows.len() as u64)
    }

    fn write_shreds(&mut self, reg: &Registry, rows: &[VerifiedShred]) -> Result<u64> {
        let Some(w) = self.shreds.as_mut() else {
            return Ok(0);
        };
        if rows.is_empty() {
            return Ok(0);
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
            sig_ok.append_option(r.sig_ok);
            merkle_ok.append_value(r.merkle_ok);
            leader.append_option(r.leader.map(|pk| pk.to_string()));
            leaf.append_option(r.data_hash.as_ref().map(hex32));
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
        w.write(&RecordBatch::try_new(shreds_schema(), cols)?)?;
        Ok(rows.len() as u64)
    }

    fn write_txns(&mut self, labels: &[(String, SourceKind)], rows: &[TxnRow]) -> Result<u64> {
        let Some(w) = self.txns.as_mut() else {
            return Ok(0);
        };
        if rows.is_empty() {
            return Ok(0);
        }
        let mut sig = StringBuilder::new();
        let mut slot = UInt64Builder::new();
        let mut source = StringBuilder::new();
        let mut mode = StringBuilder::new();
        let mut first_rx = Int64Builder::new();
        let mut created = Int64Builder::new();
        let mut dup = UInt32Builder::new();
        let mut is_vote = BooleanBuilder::new();
        let mut msg_size = UInt32Builder::new();
        let mut conn = UInt32Builder::new();

        for r in rows {
            sig.append_value(bs58::encode(&r.sig[..]).into_string());
            slot.append_value(r.slot);
            let (name, kind) = &labels[r.sid as usize];
            source.append_value(name);
            mode.append_value(kind.label());
            first_rx.append_value(r.first_rx_unix_ns);
            created.append_option(r.meta.server_created_at_ns);
            dup.append_value(r.duplicate_count);
            is_vote.append_option(r.meta.is_vote);
            msg_size.append_option(r.meta.message_size);
            conn.append_option(r.meta.connection_id);
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(sig.finish()),
            Arc::new(slot.finish()),
            Arc::new(source.finish()),
            Arc::new(mode.finish()),
            Arc::new(first_rx.finish()),
            Arc::new(created.finish()),
            Arc::new(dup.finish()),
            Arc::new(is_vote.finish()),
            Arc::new(msg_size.finish()),
            Arc::new(conn.finish()),
        ];
        w.write(&RecordBatch::try_new(txns_schema(), cols)?)?;
        Ok(rows.len() as u64)
    }

    fn finish(self, out_zip: &Path, manifest: Manifest) -> Result<PathBuf> {
        let mut members = vec!["manifest.json", SETS_FILE];
        self.sets.close().context("closing fec_sets.parquet")?;
        if let Some(w) = self.shreds {
            w.close().context("closing shreds.parquet")?;
            members.push(SHREDS_FILE);
        }
        if let Some(w) = self.txns {
            w.close().context("closing transactions.parquet")?;
            members.push(TXNS_FILE);
        }
        fs::write(self.dir.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)?;

        let file =
            File::create(out_zip).with_context(|| format!("creating {}", out_zip.display()))?;
        let mut zip = zip::ZipWriter::new(BufWriter::new(file));
        let opts: zip::write::FileOptions<()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for name in &members {
            zip.start_file(*name, opts)?;
            io::copy(&mut File::open(self.dir.join(name))?, &mut zip)?;
        }
        zip.finish()?;

        for name in &members {
            let _ = fs::remove_file(self.dir.join(name));
        }
        let _ = fs::remove_dir(&self.dir);
        Ok(out_zip.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigreg::TxnMeta;
    use arrow_array::{Array, BooleanArray, Int64Array, StringArray, UInt32Array, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn written_transactions_read_back_column_for_column() {
        let dir = std::env::temp_dir().join(format!("shreds-audit-txns-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let labels = vec![
            ("alpha".to_string(), SourceKind::Grpc),
            ("beta".to_string(), SourceKind::Shred),
        ];
        let rows = vec![
            TxnRow {
                sig: [7u8; 64],
                slot: 1_000,
                sid: 0,
                first_rx_unix_ns: 111,
                duplicate_count: 2,
                meta: TxnMeta {
                    version: None,
                    server_created_at_ns: Some(99),
                    is_vote: Some(true),
                    message_size: Some(300),
                    connection_id: Some(4),
                },
            },
            TxnRow {
                sig: [8u8; 64],
                slot: 1_001,
                sid: 1,
                first_rx_unix_ns: 222,
                duplicate_count: 0,
                meta: TxnMeta::default(),
            },
        ];

        let mut a = Archive::create(&dir, false, true).unwrap();
        assert_eq!(a.write_txns(&labels, &rows).unwrap(), 2);
        let path = dir.join(TXNS_FILE);
        a.txns.take().unwrap().close().unwrap();

        let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .expect("one batch")
            .unwrap();
        let _ = fs::remove_dir_all(&dir);

        assert_eq!(b.num_rows(), 2);
        let text = |n: &str, i: usize| {
            b.column_by_name(n)
                .unwrap_or_else(|| panic!("no column {n}"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string()
        };
        let int = |n: &str, i: usize| {
            let c = b.column_by_name(n).unwrap();
            let a = c.as_any().downcast_ref::<Int64Array>().unwrap();
            a.is_valid(i).then(|| a.value(i))
        };
        let uint = |n: &str, i: usize| {
            let c = b.column_by_name(n).unwrap();
            let a = c.as_any().downcast_ref::<UInt32Array>().unwrap();
            a.is_valid(i).then(|| a.value(i))
        };
        let flag = |n: &str, i: usize| {
            let c = b.column_by_name(n).unwrap();
            let a = c.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.is_valid(i).then(|| a.value(i))
        };
        let slot = |i: usize| {
            b.column_by_name("slot")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(i)
        };

        assert_eq!(text("signature", 0), bs58::encode(&rows[0].sig[..]).into_string());
        assert_eq!(text("source", 0), "alpha");
        assert_eq!(text("source_mode", 0), "grpc");
        assert_eq!(slot(0), 1_000);
        assert_eq!(int("first_rx_unix_ns", 0), Some(111));
        assert_eq!(int("server_created_at_ns", 0), Some(99));
        assert_eq!(uint("duplicate_count", 0), Some(2));
        assert_eq!(flag("is_vote", 0), Some(true));
        assert_eq!(uint("message_size", 0), Some(300));
        assert_eq!(uint("connection_id", 0), Some(4));

        assert_eq!(text("signature", 1), bs58::encode(&rows[1].sig[..]).into_string());
        assert_eq!(text("source", 1), "beta");
        assert_eq!(text("source_mode", 1), "shreds");
        assert_eq!(slot(1), 1_001);
        assert_eq!(int("first_rx_unix_ns", 1), Some(222));
        assert_eq!(uint("duplicate_count", 1), Some(0));
        assert_eq!(int("server_created_at_ns", 1), None);
        assert_eq!(flag("is_vote", 1), None);
        assert_eq!(uint("message_size", 1), None);
        assert_eq!(uint("connection_id", 1), None);
    }

    #[test]
    fn write_txns_without_the_flag_writes_nothing() {
        let dir = std::env::temp_dir().join(format!("shreds-audit-notxns-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut a = Archive::create(&dir, false, false).unwrap();
        let written = a
            .write_txns(
                &[("alpha".to_string(), SourceKind::Grpc)],
                &[TxnRow {
                    sig: [1u8; 64],
                    slot: 1,
                    sid: 0,
                    first_rx_unix_ns: 1,
                    duplicate_count: 0,
                    meta: TxnMeta::default(),
                }],
            )
            .unwrap();
        assert_eq!(written, 0, "no writer must report no rows landed");
        assert!(a.txns.is_none());
        assert!(!dir.join("transactions.parquet").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
