use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use postgres::{Client, NoTls};

use crate::{
    agg::SetRow,
    config::{Config, PostgresCfg},
    live::LiveStats,
    registry::{ProviderId, Registry},
};

use super::{
    manifest::{GIT_COMMIT, SCHEMA_VERSION},
    Counters, Manifest, ProviderPing, TxnSource, VERSION,
};

const QUEUE_DEPTH: usize = 64;

pub struct PgSink {
    names: Vec<String>,
    fold: IntervalFold,
    prev: Counters,
    since: Instant,
    flush_every: Duration,
    tx: Sender<Batch>,
    shutdown: Arc<AtomicBool>,
    writer: JoinHandle<()>,
    dropped: u64,
}

impl PgSink {
    pub fn open(cfg: &Config, registry: &Registry) -> Result<Self> {
        let pg = &cfg.export.postgres;
        let url = connection_url(pg)?;
        if pg.flush_secs == 0 {
            bail!(
                "config: `export.postgres.flush_secs` is 0 — that publishes in a tight loop; \
                 set the seconds per aggregation interval (default 15)"
            );
        }
        let host = super::hostname();
        let names = registry.names().to_vec();
        let run = RunInfo {
            started_at: SystemTime::now(),
            rpc_url: cfg.rpc_endpoint().map(|r| r.label()).unwrap_or_default(),
            providers: names.clone(),
        };

        match connect(&url) {
            Ok(_) => eprintln!("postgres export: connected, schema applied"),
            Err(e) => eprintln!(
                "postgres export: initial connection failed ({e:#}); capturing anyway, the writer \
                 retries every {}s",
                pg.flush_secs
            ),
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = crossbeam_channel::bounded(QUEUE_DEPTH);
        let writer = std::thread::Builder::new()
            .name("pg-writer".into())
            .spawn({
                let shutdown = shutdown.clone();
                move || writer_loop(url, host, run, rx, shutdown)
            })
            .context("spawning the postgres writer thread")?;

        Ok(Self {
            fold: IntervalFold::new(names.len()),
            names,
            prev: Counters::default(),
            since: Instant::now(),
            flush_every: Duration::from_secs(pg.flush_secs),
            tx,
            shutdown,
            writer,
            dropped: 0,
        })
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_every
    }

    pub fn write_sets(&mut self, rows: &[SetRow]) {
        self.fold.ingest(rows);
    }

    pub fn flush(&mut self, m: &Manifest) {
        let batch = Batch {
            at: SystemTime::now(),
            window_secs: self.since.elapsed().as_secs_f64(),
            providers: self.fold.rows(&self.names),
            sources: m
                .txn_compare
                .as_ref()
                .map(|t| t.sources.clone())
                .unwrap_or_default(),
            pings: m.provider_pings.clone(),
            counters: counter_deltas(&m.counters, &self.prev),
        };

        self.prev = m.counters.clone();
        self.since = Instant::now();
        self.fold.reset();

        if self.tx.try_send(batch).is_err() {
            self.dropped += 1;
        }
    }

    pub fn close(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        drop(self.tx);
        let _ = self.writer.join();
        if self.dropped > 0 {
            eprintln!(
                "postgres export: {} interval(s) were dropped because the writer could not keep \
                 up — those gaps in the dashboard are missing rows, not missing shreds",
                self.dropped
            );
        }
    }
}

pub fn connection_url(cfg: &PostgresCfg) -> Result<String> {
    choose_url(std::env::var("DATABASE_URL").ok(), &cfg.url)
}

fn choose_url(env: Option<String>, config: &str) -> Result<String> {
    let url = env
        .filter(|url| !url.trim().is_empty())
        .unwrap_or_else(|| config.to_string());
    if url.trim().is_empty() {
        bail!(
            "export mode is `postgres` but no connection string was given — set \
             `export.postgres.url` or $DATABASE_URL (postgres://user:pass@host:5432/db)"
        );
    }
    for strict in ["sslmode=require", "sslmode=verify-ca", "sslmode=verify-full"] {
        if url.contains(strict) {
            bail!(
                "the connection string asks for `{strict}`, but this build connects without TLS. \
                 Use a database reachable without SSL, or rebuild with a TLS connector"
            );
        }
    }
    Ok(url)
}

struct IntervalFold {
    live: LiveStats,
    sums: Vec<ProviderSums>,
}

#[derive(Default, Clone, Copy)]
struct ProviderSums {
    missed: u64,
    invalid: u64,
    invalid_sig: u64,
    invalid_data: u64,
    invalid_unknown: u64,
    duplicated: u64,
    sig_unverifiable: u64,
    n_data: u64,
    n_code: u64,
    fill_sum_us: f64,
    fill_n: u64,
    fill_max_us: f64,
}

impl IntervalFold {
    fn new(providers: usize) -> Self {
        Self {
            live: LiveStats::new(),
            sums: vec![ProviderSums::default(); providers],
        }
    }

    fn ingest(&mut self, rows: &[SetRow]) {
        self.live.ingest(rows);
        for r in rows {
            let s = &mut self.sums[r.provider as usize];
            s.missed += r.missed as u64;
            s.invalid += r.invalid as u64;
            s.invalid_sig += r.invalid_sig as u64;
            s.invalid_data += r.invalid_data as u64;
            s.invalid_unknown += r.invalid_unknown as u64;
            s.duplicated += r.duplicated as u64;
            s.sig_unverifiable += r.sig_unverifiable as u64;
            s.n_data += r.n_data as u64;
            s.n_code += r.n_code as u64;
            if let Some(decoded) = r.decode_ns {
                let fill = (decoded - r.first_ns).max(0) as f64 / 1000.0;
                s.fill_sum_us += fill;
                s.fill_n += 1;
                s.fill_max_us = s.fill_max_us.max(fill);
            }
        }
    }

    fn rows(&self, names: &[String]) -> Vec<ProviderRow> {
        let sets_total = self.live.total_sets();
        names
            .iter()
            .enumerate()
            .map(|(id, name)| {
                let l = self.live.provider(id as ProviderId);
                let s = self.sums[id];
                ProviderRow {
                    provider: name.clone(),
                    sets_present: l.present,
                    sets_valid: l.valid,
                    sets_total,
                    races: l.races,
                    wins: l.wins,
                    behind_sum_us: l.behind_sum_us(),
                    behind_n: l.behind_n(),
                    behind_max_us: l.behind_max_us(),
                    missed: s.missed,
                    invalid: s.invalid,
                    invalid_sig: s.invalid_sig,
                    invalid_data: s.invalid_data,
                    invalid_unknown: s.invalid_unknown,
                    duplicated: s.duplicated,
                    sig_unverifiable: s.sig_unverifiable,
                    shreds_data: s.n_data,
                    shreds_code: s.n_code,
                    fill_sum_us: s.fill_sum_us,
                    fill_n: s.fill_n,
                    fill_max_us: s.fill_max_us,
                }
            })
            .collect()
    }

    fn reset(&mut self) {
        *self = Self::new(self.sums.len());
    }
}

fn counter_deltas(now: &Counters, prev: &Counters) -> Vec<(String, i64)> {
    let now = serde_json::to_value(now).expect("Counters is a flat struct of integers");
    let prev = serde_json::to_value(prev).expect("Counters is a flat struct of integers");
    now.as_object()
        .expect("Counters serializes to an object")
        .iter()
        .map(|(name, v)| {
            let delta = v.as_u64().unwrap_or(0).saturating_sub(prev[name].as_u64().unwrap_or(0));
            (name.clone(), delta as i64)
        })
        .collect()
}

struct Batch {
    at: SystemTime,
    window_secs: f64,
    providers: Vec<ProviderRow>,
    sources: Vec<TxnSource>,
    pings: Vec<ProviderPing>,
    counters: Vec<(String, i64)>,
}

struct RunInfo {
    started_at: SystemTime,
    rpc_url: String,
    providers: Vec<String>,
}

struct ProviderRow {
    provider: String,
    sets_present: u64,
    sets_valid: u64,
    sets_total: u64,
    races: u64,
    wins: u64,
    behind_sum_us: f64,
    behind_n: u64,
    behind_max_us: f64,
    missed: u64,
    invalid: u64,
    invalid_sig: u64,
    invalid_data: u64,
    invalid_unknown: u64,
    duplicated: u64,
    sig_unverifiable: u64,
    shreds_data: u64,
    shreds_code: u64,
    fill_sum_us: f64,
    fill_n: u64,
    fill_max_us: f64,
}

fn writer_loop(
    url: String,
    host: String,
    run: RunInfo,
    rx: Receiver<Batch>,
    shutdown: Arc<AtomicBool>,
) {
    let mut client: Option<Client> = None;
    let mut degraded = false;

    for batch in rx {
        if client.is_none() {
            if shutdown.load(Ordering::Relaxed) {
                continue;
            }
            match connect(&url) {
                Ok(mut c) => {
                    if let Err(e) = write_run(&mut c, &host, &run) {
                        if !degraded {
                            eprintln!("postgres export: recording the run failed ({e:#})");
                            degraded = true;
                        }
                        continue;
                    }
                    if degraded {
                        eprintln!("postgres export: reconnected");
                        degraded = false;
                    }
                    client = Some(c);
                }
                Err(e) => {
                    if !degraded {
                        eprintln!("postgres export: {e:#} — retrying every interval");
                        degraded = true;
                    }
                    continue;
                }
            }
        }
        let Some(c) = client.as_mut() else { continue };
        if let Err(e) = write_batch(c, &host, &batch) {
            if !degraded {
                eprintln!("postgres export: write failed ({e:#}) — reconnecting");
                degraded = true;
            }
            client = None;
        }
    }
}

fn connect(url: &str) -> Result<Client> {
    let mut client = Client::connect(url, NoTls).context("connecting")?;
    client
        .batch_execute(include_str!("schema.sql"))
        .context("applying schema.sql")?;
    Ok(client)
}

const PROVIDER_SQL: &str = "\
INSERT INTO provider_stats (
    ts, host, provider, window_secs,
    sets_present, sets_valid, sets_total, races, wins,
    behind_sum_us, behind_n, behind_max_us,
    missed, invalid, invalid_sig, invalid_data, invalid_unknown,
    duplicated, sig_unverifiable,
    shreds_data, shreds_code,
    fill_sum_us, fill_n, fill_max_us
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
ON CONFLICT DO NOTHING";

const SOURCE_SQL: &str = "\
INSERT INTO txn_source_stats (
    ts, host, source, kind,
    seen, contested, winrate,
    behind_mean_us, behind_p50_us, behind_p90_us, behind_p99_us,
    onchain_slots_checked, onchain_slots_absent, onchain_txns,
    onchain_missed, onchain_corrupted, onchain_duplicated, onchain_bad, onchain_bad_pct
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT DO NOTHING";

const COUNTER_SQL: &str = "\
INSERT INTO audit_counters (ts, host, window_secs, name, value)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT DO NOTHING";

const PING_SQL: &str = "\
INSERT INTO provider_pings (ts, host, provider, ip, kind, source, rtt_ms, checked_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT DO NOTHING";

const RUN_SQL: &str = "\
INSERT INTO audit_runs (
    host, started_at, tool_version, git_commit, schema_version, rpc_url, providers
) VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT DO NOTHING";

fn write_run(client: &mut Client, host: &str, run: &RunInfo) -> Result<()> {
    client.execute(
        RUN_SQL,
        &[
            &host,
            &run.started_at,
            &VERSION,
            &GIT_COMMIT,
            &(SCHEMA_VERSION as i32),
            &run.rpc_url,
            &run.providers,
        ],
    )?;
    Ok(())
}

fn write_batch(client: &mut Client, host: &str, b: &Batch) -> Result<()> {
    let mut tx = client.transaction()?;

    for p in &b.providers {
        tx.execute(
            PROVIDER_SQL,
            &[
                &b.at,
                &host,
                &p.provider,
                &b.window_secs,
                &(p.sets_present as i64),
                &(p.sets_valid as i64),
                &(p.sets_total as i64),
                &(p.races as i64),
                &(p.wins as i64),
                &p.behind_sum_us,
                &(p.behind_n as i64),
                &p.behind_max_us,
                &(p.missed as i64),
                &(p.invalid as i64),
                &(p.invalid_sig as i64),
                &(p.invalid_data as i64),
                &(p.invalid_unknown as i64),
                &(p.duplicated as i64),
                &(p.sig_unverifiable as i64),
                &(p.shreds_data as i64),
                &(p.shreds_code as i64),
                &p.fill_sum_us,
                &(p.fill_n as i64),
                &p.fill_max_us,
            ],
        )?;
    }

    for s in &b.sources {
        tx.execute(
            SOURCE_SQL,
            &[
                &b.at,
                &host,
                &s.name,
                &s.kind.label(),
                &(s.seen as i64),
                &(s.contested as i64),
                &s.winrate,
                &s.behind_mean_us,
                &s.behind_p50_us,
                &s.behind_p90_us,
                &s.behind_p99_us,
                &(s.onchain_slots_checked as i64),
                &(s.onchain_slots_absent as i64),
                &(s.onchain_txns as i64),
                &(s.onchain_missed as i64),
                &(s.onchain_corrupted as i64),
                &(s.onchain_duplicated as i64),
                &(s.onchain_bad as i64),
                &s.onchain_bad_pct,
            ],
        )?;
    }

    for (name, value) in &b.counters {
        tx.execute(COUNTER_SQL, &[&b.at, &host, &b.window_secs, name, value])?;
    }

    for p in &b.pings {
        let checked_at = p
            .checked_at_unix_ns
            .map(|ns| std::time::UNIX_EPOCH + Duration::from_nanos(ns as u64));
        tx.execute(
            PING_SQL,
            &[
                &b.at,
                &host,
                &p.provider,
                &p.ip,
                &p.kind.label(),
                &p.source,
                &p.rtt_ms,
                &checked_at,
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(provider: ProviderId, slot: u64, decode_ns: Option<i64>, missed: u32) -> SetRow {
        SetRow {
            provider,
            slot,
            fec_set_index: 0,
            leader: None,
            first_ns: 0,
            decode_ns,
            last_ns: 0,
            n_data: 30,
            n_code: 2,
            expected_total: None,
            missed,
            invalid: 0,
            invalid_sig: 0,
            invalid_data: 0,
            invalid_unknown: 0,
            duplicated: 0,
            sig_unverifiable: 0,
            is_valid: decode_ns.is_some(),
            last_in_slot: false,
        }
    }

    #[test]
    fn an_interval_publishes_sums_and_then_forgets_them() {
        let names = vec!["alpha".to_string(), "beta".to_string()];
        let mut fold = IntervalFold::new(names.len());

        fold.ingest(&[
            row(0, 100, Some(1_000_000), 0),
            row(1, 100, Some(1_500_000), 3),
        ]);
        fold.ingest(&[row(0, 101, Some(2_000_000), 0)]);

        let rows = fold.rows(&names);
        assert_eq!(rows.len(), 2);
        let (a, b) = (&rows[0], &rows[1]);

        assert_eq!(a.provider, "alpha");
        assert_eq!(a.sets_present, 2);
        assert_eq!(a.sets_valid, 2);
        assert_eq!(a.sets_total, 2, "both sets count toward coverage");
        assert_eq!(a.races, 1, "the solo set is not a race");
        assert_eq!(a.wins, 1);
        assert_eq!(a.behind_sum_us, 0.0);
        assert_eq!(a.behind_n, 1);
        assert_eq!(a.fill_sum_us, 3_000.0, "1000 µs + 2000 µs");
        assert_eq!(a.fill_n, 2, "the solo set has a fill time even with no race");
        assert_eq!(a.fill_max_us, 2_000.0);
        assert_eq!(a.behind_max_us, 0.0, "the winner is never behind itself");
        assert_eq!((a.shreds_data, a.shreds_code), (60, 4), "two sets of 30 + 2");

        assert_eq!(b.provider, "beta");
        assert_eq!(b.sets_present, 1);
        assert_eq!(b.races, 1);
        assert_eq!(b.wins, 0);
        assert_eq!(b.behind_sum_us, 500.0, "500_000 ns behind == 500 µs");
        assert_eq!(b.behind_n, 1, "the sum's denominator travels with it");
        assert_eq!(b.missed, 3);
        assert_eq!(b.fill_sum_us, 1_500.0);
        assert_eq!(b.fill_max_us, 1_500.0);
        assert_eq!(b.behind_max_us, 500.0);
        assert_eq!((b.shreds_data, b.shreds_code), (30, 2), "one set of 30 + 2");

        fold.reset();
        let rows = fold.rows(&names);
        assert_eq!(rows.len(), 2, "a silent provider still gets a row");
        assert!(rows.iter().all(|r| r.sets_present == 0
            && r.races == 0
            && r.wins == 0
            && r.behind_n == 0
            && r.missed == 0
            && r.fill_n == 0
            && r.fill_max_us == 0.0));
        assert_eq!(rows[0].sets_total, 0);
    }

    #[test]
    fn counters_are_published_as_named_deltas() {
        let prev = Counters {
            udp_received: 100,
            udp_kernel_dropped: 5,
            ..Counters::default()
        };
        let now = Counters {
            udp_received: 175,
            udp_kernel_dropped: 5,
            shreds_parsed: 40,
            ..Counters::default()
        };

        let deltas: std::collections::HashMap<String, i64> =
            counter_deltas(&now, &prev).into_iter().collect();

        assert_eq!(deltas["udp_received"], 75, "delta, not the running total");
        assert_eq!(deltas["udp_kernel_dropped"], 0, "unchanged counters publish 0");
        assert_eq!(deltas["shreds_parsed"], 40);
        assert_eq!(
            deltas.len(),
            21,
            "every field of Counters must reach the database; add one and this moves"
        );
    }

    #[test]
    fn a_missing_url_is_refused() {
        let err = choose_url(None, "   ").unwrap_err().to_string();
        assert!(err.contains("DATABASE_URL"), "{err}");
    }

    #[test]
    fn database_url_overrides_the_config_url() {
        let config = "postgres://u:p@127.0.0.1:15432/db";
        let env = "postgres://u:p@127.0.0.1:5432/db";
        assert_eq!(choose_url(Some(env.into()), config).unwrap(), env);
        assert_eq!(choose_url(Some("  ".into()), config).unwrap(), config);
        assert_eq!(choose_url(None, config).unwrap(), config);
    }

    #[test]
    fn a_tls_only_endpoint_is_refused_with_a_reason() {
        let cfg = PostgresCfg {
            url: "postgres://u:p@db.example.com:5432/audit?sslmode=require".into(),
            flush_secs: 15,
        };
        let err = connection_url(&cfg).unwrap_err().to_string();
        assert!(err.contains("without TLS"), "{err}");
    }

    const STATEMENTS: [(&str, &str); 5] = [
        ("provider_stats", PROVIDER_SQL),
        ("txn_source_stats", SOURCE_SQL),
        ("audit_counters", COUNTER_SQL),
        ("provider_pings", PING_SQL),
        ("audit_runs", RUN_SQL),
    ];

    fn columns_and_placeholders(sql: &str) -> (Vec<&str>, Vec<&str>) {
        fn split(s: &str) -> Vec<&str> {
            s.split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .collect()
        }
        let open = sql.find('(').expect("a column list");
        let close = sql[open..].find(')').expect("a closing paren") + open;
        let cols = split(&sql[open + 1..close]);

        let tail = &sql[close..];
        let values = tail.find("VALUES").expect("a VALUES clause");
        let open = tail[values..].find('(').expect("a placeholder list") + values;
        let close = tail[open..].find(')').expect("a closing paren") + open;
        (cols, split(&tail[open + 1..close]))
    }

    #[test]
    fn every_insert_binds_each_column_to_its_own_placeholder() {
        for (table, sql) in STATEMENTS {
            let (cols, params) = columns_and_placeholders(sql);
            assert_eq!(
                cols.len(),
                params.len(),
                "{table}: {} columns but {} placeholders",
                cols.len(),
                params.len()
            );
            for (i, p) in params.iter().enumerate() {
                assert_eq!(
                    *p,
                    format!("${}", i + 1),
                    "{table}: placeholder {} is `{p}` — they must run $1..$N in order",
                    i + 1
                );
            }
        }
    }

    fn schema_columns(table: &str) -> Vec<String> {
        let sql = include_str!("schema.sql");
        let head = format!("CREATE TABLE IF NOT EXISTS {table} (");
        let start = sql
            .find(&head)
            .unwrap_or_else(|| panic!("schema.sql declares no table {table}"))
            + head.len();
        let body = &sql[start..];
        let end = body.find("\n);").expect("a table that ends");
        body[..end]
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with("--") && !l.starts_with("PRIMARY KEY"))
            .filter_map(|l| l.split_whitespace().next())
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn every_insert_matches_the_schema_column_for_column() {
        for (table, sql) in STATEMENTS {
            let mut declared = schema_columns(table);
            let (used, _) = columns_and_placeholders(sql);
            let mut used: Vec<String> = used.into_iter().map(str::to_string).collect();
            declared.sort();
            used.sort();
            assert_eq!(
                used, declared,
                "{table}: the INSERT and schema.sql disagree on which columns exist"
            );
        }
    }

    #[test]
    #[ignore = "needs a postgres; set TEST_DATABASE_URL and run with --ignored"]
    fn a_written_batch_reads_back_column_for_column() {
        use crate::sigreg::SourceKind;

        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL");
        let mut client = connect(&url).expect("connect and apply the schema");

        let at = std::time::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        let host = "test-host";
        let clean = |c: &mut Client| {
            for t in [
                "provider_stats",
                "txn_source_stats",
                "audit_counters",
                "provider_pings",
                "audit_runs",
            ] {
                c.execute(&format!("DELETE FROM {t} WHERE host = $1"), &[&host])
                    .unwrap();
            }
        };
        clean(&mut client);

        let batch = Batch {
            at,
            window_secs: 12.5,
            providers: vec![ProviderRow {
                provider: "alpha".into(),
                sets_present: 1,
                sets_valid: 2,
                sets_total: 3,
                races: 4,
                wins: 5,
                behind_sum_us: 6.5,
                behind_n: 7,
                missed: 8,
                invalid: 9,
                invalid_sig: 10,
                invalid_data: 11,
                invalid_unknown: 12,
                duplicated: 13,
                sig_unverifiable: 14,
                behind_max_us: 15.5,
                shreds_data: 16,
                shreds_code: 17,
                fill_sum_us: 18.5,
                fill_n: 19,
                fill_max_us: 20.5,
            }],
            sources: vec![TxnSource {
                name: "grpc-a".into(),
                kind: SourceKind::Grpc,
                seen: 21,
                contested: 22,
                winrate: Some(0.23),
                behind_mean_us: Some(24.0),
                behind_p50_us: Some(25.0),
                behind_p90_us: Some(26.0),
                behind_p99_us: Some(27.0),
                onchain_slots_checked: 28,
                onchain_slots_absent: 29,
                onchain_txns: 30,
                onchain_missed: 31,
                onchain_corrupted: 32,
                onchain_duplicated: 33,
                onchain_bad: 34,
                onchain_bad_pct: Some(0.35),
            }],
            pings: vec![],
            counters: vec![("udp_kernel_dropped".into(), 41)],
        };
        write_batch(&mut client, host, &batch).expect("write");

        let p = client
            .query_one(
                "SELECT * FROM provider_stats WHERE host = $1 AND provider = 'alpha'",
                &[&host],
            )
            .unwrap();
        assert_eq!(p.get::<_, f64>("window_secs"), 12.5);
        assert_eq!(p.get::<_, i64>("sets_present"), 1);
        assert_eq!(p.get::<_, i64>("sets_valid"), 2);
        assert_eq!(p.get::<_, i64>("sets_total"), 3);
        assert_eq!(p.get::<_, i64>("races"), 4);
        assert_eq!(p.get::<_, i64>("wins"), 5);
        assert_eq!(p.get::<_, f64>("behind_sum_us"), 6.5);
        assert_eq!(p.get::<_, i64>("behind_n"), 7);
        assert_eq!(p.get::<_, i64>("missed"), 8);
        assert_eq!(p.get::<_, i64>("invalid"), 9);
        assert_eq!(p.get::<_, i64>("invalid_sig"), 10);
        assert_eq!(p.get::<_, i64>("invalid_data"), 11);
        assert_eq!(p.get::<_, i64>("invalid_unknown"), 12);
        assert_eq!(p.get::<_, i64>("duplicated"), 13);
        assert_eq!(p.get::<_, i64>("sig_unverifiable"), 14);
        assert_eq!(p.get::<_, f64>("behind_max_us"), 15.5);
        assert_eq!(p.get::<_, i64>("shreds_data"), 16);
        assert_eq!(p.get::<_, i64>("shreds_code"), 17);
        assert_eq!(p.get::<_, f64>("fill_sum_us"), 18.5);
        assert_eq!(p.get::<_, i64>("fill_n"), 19);
        assert_eq!(p.get::<_, f64>("fill_max_us"), 20.5);

        let s = client
            .query_one("SELECT * FROM txn_source_stats WHERE host = $1", &[&host])
            .unwrap();
        assert_eq!(s.get::<_, String>("source"), "grpc-a");
        assert_eq!(s.get::<_, String>("kind"), "grpc");
        assert_eq!(s.get::<_, i64>("seen"), 21);
        assert_eq!(s.get::<_, i64>("contested"), 22);
        assert_eq!(s.get::<_, Option<f64>>("winrate"), Some(0.23));
        assert_eq!(s.get::<_, Option<f64>>("behind_mean_us"), Some(24.0));
        assert_eq!(s.get::<_, Option<f64>>("behind_p50_us"), Some(25.0));
        assert_eq!(s.get::<_, Option<f64>>("behind_p90_us"), Some(26.0));
        assert_eq!(s.get::<_, Option<f64>>("behind_p99_us"), Some(27.0));
        assert_eq!(s.get::<_, i64>("onchain_slots_checked"), 28);
        assert_eq!(s.get::<_, i64>("onchain_slots_absent"), 29);
        assert_eq!(s.get::<_, i64>("onchain_txns"), 30);
        assert_eq!(s.get::<_, i64>("onchain_missed"), 31);
        assert_eq!(s.get::<_, i64>("onchain_corrupted"), 32);
        assert_eq!(s.get::<_, i64>("onchain_duplicated"), 33);
        assert_eq!(s.get::<_, i64>("onchain_bad"), 34);
        assert_eq!(s.get::<_, Option<f64>>("onchain_bad_pct"), Some(0.35));

        let c = client
            .query_one("SELECT * FROM audit_counters WHERE host = $1", &[&host])
            .unwrap();
        assert_eq!(c.get::<_, String>("name"), "udp_kernel_dropped");
        assert_eq!(c.get::<_, i64>("value"), 41);
        assert_eq!(c.get::<_, f64>("window_secs"), 12.5);

        clean(&mut client);
    }
}
