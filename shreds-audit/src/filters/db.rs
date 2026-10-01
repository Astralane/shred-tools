use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use postgres::{Client, NoTls};

const QUEUE_DEPTH: usize = 20_000;
const RETRY_CAP: usize = 50_000;
const BATCH_MAX: usize = 2_000;
const BATCH_WAIT: Duration = Duration::from_secs(2);
const RECONNECT_EVERY: Duration = Duration::from_secs(10);

pub struct WindowRow {
    pub source: String,
    pub kind: &'static str,
    pub commitment: Option<String>,
    pub connection_id: u32,
    pub started_at: SystemTime,
    pub ended_at: SystemTime,
    pub duration_secs: f64,
    pub bundle: String,
    pub filters_json: String,
    pub end_reason: String,
    pub connect_ms: Option<f64>,
    pub first_msg_ms: Option<f64>,
    pub tip_open: Option<u64>,
    pub tip_close: Option<u64>,
    pub audit_start_slot: Option<u64>,
    pub audit_end_slot: Option<u64>,
    pub delivered: u64,
    pub tagged_json: String,
    pub untagged_updates: u64,
    pub unknown_tags: u64,
    pub duplicates: u64,
    pub tag_false_positive: u64,
    pub tag_missing: u64,
    pub vote_flag_mismatch: u64,
    pub slots_checked: u64,
    pub slots_skipped: u64,
    pub slots_unchecked: u64,
    pub expected: u64,
    pub matched: u64,
    pub missed: u64,
    pub extra_in_block: u64,
    pub extra_not_in_block: u64,
    pub contested: u64,
    pub wins: u64,
    pub behind_us: [Option<f64>; 3],
    pub vs_shred_n: u64,
    pub vs_shred_us: [Option<f64>; 4],
    pub server_delay_us: [Option<f64>; 3],
}

pub struct SlotCheckRow {
    pub ts: SystemTime,
    pub source: String,
    pub kind: &'static str,
    pub window_started_at: SystemTime,
    pub bundle: String,
    pub filter: String,
    pub slot: u64,
    pub block_status: &'static str,
    pub block_txs: u32,
    pub expected: u32,
    pub delivered: u32,
    pub matched: u32,
    pub missed: u32,
    pub extra_in_block: u32,
    pub extra_not_in_block: u32,
}

pub struct ViolationRow {
    pub ts: SystemTime,
    pub source: String,
    pub kind: &'static str,
    pub window_started_at: SystemTime,
    pub bundle: String,
    pub filter: String,
    pub slot: u64,
    pub signature: String,
    pub violation: &'static str,
    pub reason: String,
}

pub struct FetchRow {
    pub ts: SystemTime,
    pub rpc: String,
    pub slot: u64,
    pub attempt: u32,
    pub status: &'static str,
    pub latency_ms: f64,
    pub block_txs: Option<u32>,
    pub error: Option<String>,
}

pub enum Row {
    Window(Box<WindowRow>),
    Slot(SlotCheckRow),
    Violation(ViolationRow),
    Fetch(FetchRow),
}

#[derive(Clone)]
pub struct DbHandle {
    tx: Option<Sender<Row>>,
    dropped: Arc<AtomicU64>,
}

impl DbHandle {
    pub fn disabled() -> Self {
        Self {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn send(&self, row: Row) {
        let Some(tx) = &self.tx else { return };
        if tx.try_send(row).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub fn spawn_writer(url: String) -> Result<(DbHandle, JoinHandle<()>)> {
    let (tx, rx) = crossbeam_channel::bounded(QUEUE_DEPTH);
    let host = crate::out::hostname();
    let handle = std::thread::Builder::new()
        .name("filter-db".into())
        .spawn(move || writer_loop(url, host, rx))
        .context("spawning the filter-audit writer")?;
    let db = DbHandle {
        tx: Some(tx),
        dropped: Arc::new(AtomicU64::new(0)),
    };
    Ok((db, handle))
}

fn writer_loop(url: String, host: String, rx: Receiver<Row>) {
    let mut client: Option<Client> = None;
    let mut last_attempt: Option<Instant> = None;
    let mut pending: Vec<Row> = Vec::new();
    let mut degraded = false;
    let mut closed = false;

    while !closed || !pending.is_empty() {
        let deadline = Instant::now() + BATCH_WAIT;
        while pending.len() < BATCH_MAX && !closed {
            let wait = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(wait) {
                Ok(r) => pending.push(r),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => closed = true,
            }
        }
        if pending.is_empty() {
            continue;
        }
        if pending.len() > RETRY_CAP {
            let excess = pending.len() - RETRY_CAP;
            pending.drain(..excess);
        }

        if client.is_none() {
            if last_attempt.is_some_and(|t| t.elapsed() < RECONNECT_EVERY) {
                if closed {
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            last_attempt = Some(Instant::now());
            match connect(&url) {
                Ok(c) => {
                    if degraded {
                        eprintln!("filter-audit db: reconnected");
                        degraded = false;
                    }
                    client = Some(c);
                }
                Err(e) => {
                    if !degraded {
                        eprintln!("filter-audit db: {e:#} — retrying every {RECONNECT_EVERY:?}");
                        degraded = true;
                    }
                    if closed {
                        break;
                    }
                    continue;
                }
            }
        }
        let c = client.as_mut().expect("connected above");
        match write_batch(c, &host, &pending) {
            Ok(()) => pending.clear(),
            Err(e) => {
                if !degraded {
                    eprintln!("filter-audit db: write failed ({e:#}) — reconnecting");
                    degraded = true;
                }
                client = None;
                last_attempt = Some(Instant::now());
                if closed {
                    break;
                }
            }
        }
    }
}

fn connect(url: &str) -> Result<Client> {
    let mut client = Client::connect(url, NoTls).context("connecting")?;
    client
        .batch_execute(include_str!("schema.sql"))
        .context("applying filters/schema.sql")?;
    Ok(client)
}

const WINDOW_SQL: &str = "\
INSERT INTO filter_windows (
    host, source, kind, commitment, connection_id, started_at, ended_at, duration_secs,
    bundle, filters, end_reason, connect_ms, first_msg_ms, tip_open, tip_close,
    audit_start_slot, audit_end_slot, delivered, tagged, untagged_updates, unknown_tags,
    duplicates, tag_false_positive, tag_missing, vote_flag_mismatch,
    slots_checked, slots_skipped, slots_unchecked, expected, matched, missed,
    extra_in_block, extra_not_in_block, contested, wins,
    behind_p50_us, behind_p90_us, behind_p99_us,
    vs_shred_n, vs_shred_p10_us, vs_shred_p50_us, vs_shred_p90_us, vs_shred_p99_us,
    server_delay_p50_us, server_delay_p90_us, server_delay_p99_us
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::jsonb,$11,$12,$13,$14,$15,$16,$17,$18,
    $19::text::jsonb,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,
    $38,$39,$40,$41,$42,$43,$44,$45,$46)
ON CONFLICT DO NOTHING";

const SLOT_SQL: &str = "\
INSERT INTO filter_slot_checks (
    ts, host, source, kind, window_started_at, bundle, filter, slot, block_status,
    block_txs, expected, delivered, matched, missed, extra_in_block, extra_not_in_block
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT DO NOTHING";

const VIOLATION_SQL: &str = "\
INSERT INTO filter_violations (
    ts, host, source, kind, window_started_at, bundle, filter, slot, signature, violation, reason
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT DO NOTHING";

const FETCH_SQL: &str = "\
INSERT INTO rpc_block_fetches (
    ts, host, rpc, slot, attempt, status, latency_ms, block_txs, error
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT DO NOTHING";

fn i(v: u64) -> i64 {
    v.min(i64::MAX as u64) as i64
}

fn oi(v: Option<u64>) -> Option<i64> {
    v.map(i)
}

fn write_batch(c: &mut Client, host: &str, rows: &[Row]) -> Result<()> {
    let mut t = c.transaction()?;
    let window = t.prepare(WINDOW_SQL)?;
    let slot = t.prepare(SLOT_SQL)?;
    let violation = t.prepare(VIOLATION_SQL)?;
    let fetch = t.prepare(FETCH_SQL)?;
    for row in rows {
        match row {
            Row::Window(w) => {
                t.execute(
                    &window,
                    &[
                        &host,
                        &w.source,
                        &w.kind,
                        &w.commitment,
                        &(w.connection_id as i64),
                        &w.started_at,
                        &w.ended_at,
                        &w.duration_secs,
                        &w.bundle,
                        &w.filters_json,
                        &w.end_reason,
                        &w.connect_ms,
                        &w.first_msg_ms,
                        &oi(w.tip_open),
                        &oi(w.tip_close),
                        &oi(w.audit_start_slot),
                        &oi(w.audit_end_slot),
                        &i(w.delivered),
                        &w.tagged_json,
                        &i(w.untagged_updates),
                        &i(w.unknown_tags),
                        &i(w.duplicates),
                        &i(w.tag_false_positive),
                        &i(w.tag_missing),
                        &i(w.vote_flag_mismatch),
                        &i(w.slots_checked),
                        &i(w.slots_skipped),
                        &i(w.slots_unchecked),
                        &i(w.expected),
                        &i(w.matched),
                        &i(w.missed),
                        &i(w.extra_in_block),
                        &i(w.extra_not_in_block),
                        &i(w.contested),
                        &i(w.wins),
                        &w.behind_us[0],
                        &w.behind_us[1],
                        &w.behind_us[2],
                        &i(w.vs_shred_n),
                        &w.vs_shred_us[0],
                        &w.vs_shred_us[1],
                        &w.vs_shred_us[2],
                        &w.vs_shred_us[3],
                        &w.server_delay_us[0],
                        &w.server_delay_us[1],
                        &w.server_delay_us[2],
                    ],
                )?;
            }
            Row::Slot(s) => {
                t.execute(
                    &slot,
                    &[
                        &s.ts,
                        &host,
                        &s.source,
                        &s.kind,
                        &s.window_started_at,
                        &s.bundle,
                        &s.filter,
                        &i(s.slot),
                        &s.block_status,
                        &(s.block_txs as i32),
                        &(s.expected as i32),
                        &(s.delivered as i32),
                        &(s.matched as i32),
                        &(s.missed as i32),
                        &(s.extra_in_block as i32),
                        &(s.extra_not_in_block as i32),
                    ],
                )?;
            }
            Row::Violation(v) => {
                t.execute(
                    &violation,
                    &[
                        &v.ts,
                        &host,
                        &v.source,
                        &v.kind,
                        &v.window_started_at,
                        &v.bundle,
                        &v.filter,
                        &i(v.slot),
                        &v.signature,
                        &v.violation,
                        &v.reason,
                    ],
                )?;
            }
            Row::Fetch(f) => {
                t.execute(
                    &fetch,
                    &[
                        &f.ts,
                        &host,
                        &f.rpc,
                        &i(f.slot),
                        &(f.attempt as i32),
                        &f.status,
                        &f.latency_ms,
                        &f.block_txs.map(|n| n as i32),
                        &f.error,
                    ],
                )?;
            }
        }
    }
    t.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn every_row_kind_lands_in_postgres() {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let source = format!(
            "itest-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let now = SystemTime::now();
        let (h, w) = spawn_writer(url.clone()).unwrap();
        h.send(Row::Window(Box::new(WindowRow {
            source: source.clone(),
            kind: "grpc-deshred",
            commitment: None,
            connection_id: 3,
            started_at: now,
            ended_at: now,
            duration_secs: 42.0,
            bundle: "votes_split".into(),
            filters_json: r#"{"votes":{"vote":true}}"#.into(),
            end_reason: "rotated".into(),
            connect_ms: Some(12.5),
            first_msg_ms: None,
            tip_open: Some(100),
            tip_close: Some(200),
            audit_start_slot: Some(110),
            audit_end_slot: Some(190),
            delivered: 5,
            tagged_json: r#"{"votes":5}"#.into(),
            untagged_updates: 0,
            unknown_tags: 0,
            duplicates: 1,
            tag_false_positive: 0,
            tag_missing: 2,
            vote_flag_mismatch: 0,
            slots_checked: 9,
            slots_skipped: 1,
            slots_unchecked: 0,
            expected: 100,
            matched: 99,
            missed: 1,
            extra_in_block: 0,
            extra_not_in_block: 3,
            contested: 50,
            wins: 20,
            behind_us: [Some(1.0), Some(2.0), None],
            vs_shred_n: 50,
            vs_shred_us: [Some(-5.0), Some(1.0), Some(3.0), Some(9.0)],
            server_delay_us: [None, None, None],
        })));
        h.send(Row::Slot(SlotCheckRow {
            ts: now,
            source: source.clone(),
            kind: "grpc-deshred",
            window_started_at: now,
            bundle: "votes_split".into(),
            filter: "votes".into(),
            slot: 110,
            block_status: "ok",
            block_txs: 1200,
            expected: 900,
            delivered: 899,
            matched: 899,
            missed: 1,
            extra_in_block: 0,
            extra_not_in_block: 0,
        }));
        h.send(Row::Violation(ViolationRow {
            ts: now,
            source: source.clone(),
            kind: "grpc-deshred",
            window_started_at: now,
            bundle: "votes_split".into(),
            filter: "votes".into(),
            slot: 110,
            signature: "sig".into(),
            violation: "missed",
            reason: "not delivered".into(),
        }));
        h.send(Row::Fetch(FetchRow {
            ts: now,
            rpc: source.clone(),
            slot: 110,
            attempt: 1,
            status: "ok",
            latency_ms: 321.0,
            block_txs: Some(1200),
            error: None,
        }));
        drop(h);
        w.join().unwrap();

        let mut c = Client::connect(&url, NoTls).unwrap();
        let count =
            |c: &mut Client, sql: &str| -> i64 { c.query_one(sql, &[&source]).unwrap().get(0) };
        assert_eq!(
            count(
                &mut c,
                "SELECT COUNT(*) FROM filter_windows WHERE source = $1"
            ),
            1
        );
        assert_eq!(
            count(
                &mut c,
                "SELECT COUNT(*) FROM filter_slot_checks WHERE source = $1"
            ),
            1
        );
        assert_eq!(
            count(
                &mut c,
                "SELECT COUNT(*) FROM filter_violations WHERE source = $1"
            ),
            1
        );
        assert_eq!(
            count(
                &mut c,
                "SELECT COUNT(*) FROM rpc_block_fetches WHERE rpc = $1"
            ),
            1
        );
        let tagged: i64 = c
            .query_one(
                "SELECT (tagged->>'votes')::bigint FROM filter_windows WHERE source = $1",
                &[&source],
            )
            .unwrap()
            .get(0);
        assert_eq!(tagged, 5, "jsonb columns hold real JSON");
    }
}
