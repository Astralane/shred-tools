use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::Config;

#[derive(Debug, Deserialize)]
pub struct WindowStats {
    pub pairs: u64,
    pub wins: u64,
    pub p50_ms: Option<f64>,
    pub p90_ms: Option<f64>,
    pub late_count: u64,
    pub worst_ms: Option<f64>,
}

impl WindowStats {
    fn win_rate(&self) -> f64 {
        if self.pairs == 0 {
            return 0.0;
        }
        self.wins as f64 / self.pairs as f64
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Alert {
    Degraded {
        win_rate: f64,
        target_win_rate: f64,
        p50_ms: f64,
        p90_ms: f64,
        late_count: u64,
        late_rate: f64,
        late_ms_threshold: u64,
        worst_ms: f64,
        pairs: u64,
        window_secs: u64,
    },
    Recovered {
        win_rate: f64,
        pairs: u64,
        window_secs: u64,
    },
    NoData {
        minutes: u64,
        pairs: u64,
    },
}

pub struct Checker {
    config: Config,
    client: reqwest::Client,
    degraded: bool,
    last_alert: Option<Instant>,
    empty_since: Option<Instant>,
    no_data_alerted: bool,
}

impl Checker {
    pub fn new(config: Config) -> Self {
        Checker {
            config,
            client: reqwest::Client::new(),
            degraded: false,
            last_alert: None,
            empty_since: None,
            no_data_alerted: false,
        }
    }

    pub async fn tick(&mut self) -> Result<Option<Alert>> {
        let stats = self.fetch_window().await?;
        Ok(self.evaluate(stats))
    }

    pub async fn fetch_window(&self) -> Result<WindowStats> {
        let timeout = self.config.query_timeout_secs;
        let response = self
            .client
            .post(&self.config.clickhouse.url)
            .query(&[
                ("default_format", "JSONEachRow"),
                ("output_format_json_quote_64bit_integers", "0"),
                ("max_execution_time", &timeout.to_string()),
            ])
            .header("X-ClickHouse-User", &self.config.clickhouse.user)
            .header("X-ClickHouse-Key", &self.config.clickhouse.password)
            .body(self.build_query())
            .timeout(Duration::from_secs(timeout + 5))
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("clickhouse {}: {}", status, body.trim()));
        }
        let line = body
            .lines()
            .find(|l| !l.trim().is_empty())
            .ok_or_else(|| anyhow!("clickhouse returned no rows"))?;
        serde_json::from_str::<WindowStats>(line)
            .map_err(|e| anyhow!("parsing clickhouse row: {}: {}", e, line))
    }

    pub fn build_query(&self) -> String {
        let window_start = self.config.window_offset_secs + self.config.window_secs;
        let window_end = self.config.window_offset_secs;
        let db = &self.config.clickhouse.database;
        let endpoint_filter = if self.config.endpoints.is_empty() {
            String::new()
        } else {
            let list = self
                .config
                .endpoints
                .iter()
                .map(|e| format!("'{}'", e.replace('\'', "")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AND endpoint IN ({})", list)
        };
        let (ra_host, tx_host) = match &self.config.shred_host {
            Some(host) => {
                let host = host.replace('\'', "");
                (
                    format!("AND host = '{}'", host),
                    format!("AND t.host = '{}'", host),
                )
            }
            None => (String::new(), String::new()),
        };

        format!(
            r#"
WITH
    toUInt64(toUnixTimestamp(now() - INTERVAL {window_start} SECOND)) * 1000000000 AS t0,
    toUInt64(toUnixTimestamp(now() - INTERVAL {window_end} SECOND)) * 1000000000 AS t1,
    ra AS
    (
        SELECT sig, min(recv_ns) AS relay_ns
        FROM {db}.relay_arrivals
        WHERE day >= today() - 1
          AND recv_ns >= t0
          AND recv_ns <  t1
          {ra_host}
          {endpoint_filter}
        GROUP BY sig
    ),
    sh AS
    (
        SELECT t.sig AS sig, min(f.last_shred_ns) AS last_ns
        FROM {db}.tx_shreds AS t
        INNER JOIN {db}.fec_arrivals AS f
            ON f.slot = t.slot AND f.fec_set_index = t.fec_set_index AND f.host = t.host
        WHERE t.day >= today() - 1
          {tx_host}
          AND t.sig IN (SELECT sig FROM ra)
          AND f.first_shred_ns >= t0 - 60000000000
          AND f.first_shred_ns <  t1 + {lookahead_ns}
        GROUP BY t.sig
    )
SELECT
    count()                       AS pairs,
    countIf(lead_ms > 0)          AS wins,
    quantile(0.5)(lead_ms)        AS p50_ms,
    quantile(0.9)(lead_ms)        AS p90_ms,
    countIf(lead_ms < -{late_ms}) AS late_count,
    min(lead_ms)                  AS worst_ms
FROM
(
    SELECT (toFloat64(sh.last_ns) - toFloat64(ra.relay_ns)) / 1e6 AS lead_ms
    FROM sh
    INNER JOIN ra ON ra.sig = sh.sig
)
"#,
            db = db,
            window_start = window_start,
            window_end = window_end,
            ra_host = ra_host,
            tx_host = tx_host,
            endpoint_filter = endpoint_filter,
            lookahead_ns = self.config.land_lookahead_secs as u128 * 1_000_000_000,
            late_ms = self.config.late_ms_threshold,
        )
    }

    pub fn evaluate(&mut self, stats: WindowStats) -> Option<Alert> {
        if stats.pairs < self.config.min_pairs {
            return self.handle_thin_window(stats.pairs);
        }
        self.empty_since = None;
        self.no_data_alerted = false;

        let win_rate = stats.win_rate();
        let late_rate = stats.late_count as f64 / stats.pairs as f64;
        info!(
            "postpack window: {} pairs, win rate {:.1}%, p50 {:.1}ms, p90 {:.1}ms, {} late ({:.1}%)",
            stats.pairs,
            win_rate * 100.0,
            stats.p50_ms.unwrap_or_default(),
            stats.p90_ms.unwrap_or_default(),
            stats.late_count,
            late_rate * 100.0
        );

        let breached = win_rate < self.config.win_rate_threshold
            || late_rate > self.config.late_rate_threshold;

        if breached {
            let due = match self.last_alert {
                None => true,
                Some(at) => at.elapsed() >= Duration::from_secs(self.config.repeat_alert_secs),
            };
            let fire = !self.degraded || due;
            self.degraded = true;
            if fire {
                self.last_alert = Some(Instant::now());
                return Some(Alert::Degraded {
                    win_rate,
                    target_win_rate: self.config.win_rate_threshold,
                    p50_ms: stats.p50_ms.unwrap_or_default(),
                    p90_ms: stats.p90_ms.unwrap_or_default(),
                    late_count: stats.late_count,
                    late_rate,
                    late_ms_threshold: self.config.late_ms_threshold,
                    worst_ms: stats.worst_ms.unwrap_or_default(),
                    pairs: stats.pairs,
                    window_secs: self.config.window_secs,
                });
            }
            return None;
        }

        if self.degraded && win_rate >= self.config.recovery_win_rate {
            self.degraded = false;
            self.last_alert = None;
            return Some(Alert::Recovered {
                win_rate,
                pairs: stats.pairs,
                window_secs: self.config.window_secs,
            });
        }
        None
    }

    fn handle_thin_window(&mut self, pairs: u64) -> Option<Alert> {
        warn!(
            "postpack window had {} matched pairs, below the {} minimum; verdict withheld",
            pairs, self.config.min_pairs
        );
        let since = *self.empty_since.get_or_insert_with(Instant::now);
        if !self.no_data_alerted
            && since.elapsed() >= Duration::from_secs(self.config.no_data_alert_after_secs)
        {
            self.no_data_alerted = true;
            return Some(Alert::NoData {
                minutes: since.elapsed().as_secs() / 60,
                pairs,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClickhouseConfig, Config};

    fn checker() -> Checker {
        Checker::new(Config {
            clickhouse: ClickhouseConfig {
                url: "http://localhost:18123/".to_string(),
                user: "u".to_string(),
                password: "p".to_string(),
                database: "shred_indexer".to_string(),
            },
            slack_webhook: "https://slack.invalid/hook".to_string(),
            shred_host: Some("ny".to_string()),
            endpoints: vec!["64.130.45.19:30001".to_string()],
            poll_interval_secs: 60,
            window_secs: 900,
            window_offset_secs: 300,
            min_pairs: 100,
            win_rate_threshold: 0.85,
            recovery_win_rate: 0.90,
            late_ms_threshold: 100,
            late_rate_threshold: 0.10,
            repeat_alert_secs: 900,
            no_data_alert_after_secs: 1800,
            land_lookahead_secs: 120,
            query_timeout_secs: 60,
        })
    }

    fn stats(pairs: u64, wins: u64, late_count: u64) -> WindowStats {
        WindowStats {
            pairs,
            wins,
            p50_ms: Some(5.1),
            p90_ms: Some(16.2),
            late_count,
            worst_ms: Some(-927.0),
        }
    }

    #[test]
    fn breach_alerts_once_until_the_repeat_interval_elapses() {
        let mut svc = checker();
        assert!(matches!(
            svc.evaluate(stats(1000, 760, 0)),
            Some(Alert::Degraded { .. })
        ));
        assert!(svc.evaluate(stats(1000, 750, 0)).is_none());
    }

    #[test]
    fn tail_lag_breaches_even_when_the_win_rate_is_healthy() {
        let mut svc = checker();
        assert!(matches!(
            svc.evaluate(stats(1000, 990, 150)),
            Some(Alert::Degraded { .. })
        ));
    }

    #[test]
    fn the_observed_baseline_tail_rate_is_not_a_breach() {
        let mut svc = checker();
        assert!(svc.evaluate(stats(862, 813, 36)).is_none());
        assert!(!svc.degraded);
    }

    #[test]
    fn recovery_needs_the_higher_threshold_not_merely_the_absence_of_a_breach() {
        let mut svc = checker();
        svc.evaluate(stats(1000, 760, 0));
        assert!(svc.evaluate(stats(1000, 870, 0)).is_none());
        assert!(svc.degraded);
        assert!(matches!(
            svc.evaluate(stats(1000, 910, 0)),
            Some(Alert::Recovered { .. })
        ));
        assert!(!svc.degraded);
    }

    #[test]
    fn a_thin_window_never_flips_the_verdict() {
        let mut svc = checker();
        svc.evaluate(stats(1000, 760, 0));
        assert!(svc.evaluate(stats(3, 0, 0)).is_none());
        assert!(svc.degraded);
    }

    #[test]
    fn query_races_both_feeds_on_one_host_clock() {
        let sql = checker().build_query();
        assert!(sql.contains("now() - INTERVAL 1200 SECOND"));
        assert!(sql.contains("now() - INTERVAL 300 SECOND"));
        assert!(sql.contains("shred_indexer.relay_arrivals"));
        assert!(sql.contains("endpoint IN ('64.130.45.19:30001')"));
        assert!(sql.contains("AND t.host = 'ny'"));
        assert!(sql.contains("t1 + 120000000000"));
        assert!(sql.contains("countIf(lead_ms < -100)"));
        assert!(!sql.contains("validator_packet_events"));
    }

    #[tokio::test]
    async fn fetches_and_parses_a_clickhouse_window() {
        let (url, rx) = crate::test_support::stub(
            "{\"pairs\":862,\"wins\":813,\"p50_ms\":97.9,\"p90_ms\":471.7,\"late_count\":36,\"worst_ms\":-236.9}\n",
        )
        .await;
        let mut svc = checker();
        svc.config.clickhouse.url = url;

        let stats = svc.fetch_window().await.unwrap();
        assert_eq!(stats.pairs, 862);
        assert_eq!(stats.wins, 813);
        assert_eq!(stats.late_count, 36);
        assert_eq!(stats.worst_ms, Some(-236.9));

        let req = rx.await.unwrap().to_lowercase();
        assert!(req.contains("x-clickhouse-user"), "req was: {req}");
        assert!(req.contains("relay_arrivals"), "req was: {req}");
        assert!(req.contains("jsoneachrow"), "req was: {req}");
    }

    #[tokio::test]
    async fn a_degraded_window_travels_from_clickhouse_to_an_alert() {
        let (url, _rx) = crate::test_support::stub(
            "{\"pairs\":700,\"wins\":490,\"p50_ms\":-12.0,\"p90_ms\":-3.0,\"late_count\":210,\"worst_ms\":-927.0}\n",
        )
        .await;
        let mut svc = checker();
        svc.config.clickhouse.url = url;

        match svc.tick().await.unwrap() {
            Some(Alert::Degraded {
                win_rate,
                late_count,
                pairs,
                ..
            }) => {
                assert_eq!(pairs, 700);
                assert_eq!(late_count, 210);
                assert!((win_rate - 0.7).abs() < 1e-9);
            }
            other => panic!("expected a degradation alert, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_clickhouse_error_is_surfaced_not_parsed_as_a_healthy_window() {
        let (url, _rx) = crate::test_support::stub("Code: 60. DB::Exception: Unknown table").await;
        let mut svc = checker();
        svc.config.clickhouse.url = url;
        assert!(svc.fetch_window().await.is_err());
    }
}
