//! Prometheus metrics for the monitor, backing the "Shreds monitor" Grafana
//! dashboard. Per-provider counters are pre-resolved into a HashMap at startup
//! so the per-shred hot path never touches the label-set registry.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tracing::{info, warn};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ProviderLabels {
    provider: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TableLabels {
    pub table: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: String,
}

pub struct ProviderMetrics {
    pub received: Counter,
    pub invalid: Counter,
    pub duplicate: Counter,
    pub fec_sets_won: Counter,
    /// First-shred delay vs the baseline provider, in ns (negative = earlier
    /// than baseline).
    pub first_shred_delay_ns: Histogram,
}

pub struct Metrics {
    registry: Registry,
    providers: HashMap<u32, ProviderMetrics>,
    pub fec_channel_full: Counter,
    rows_written: Family<TableLabels, Counter>,
    write_errors: Family<TableLabels, Counter>,
    last_seen_slot: Gauge,
    uptime_seconds: Gauge<f64, AtomicU64>,
    started: Instant,
    current_slot: Arc<AtomicU64>,
}

/// -100ms .. +100ms around the baseline provider.
fn delay_buckets() -> impl Iterator<Item = f64> {
    [
        -100e6, -50e6, -20e6, -10e6, -5e6, -2e6, -1e6, -500e3, -200e3, 0.0, 200e3, 500e3, 1e6,
        2e6, 5e6, 10e6, 20e6, 50e6, 100e6,
    ]
    .into_iter()
}

impl Metrics {
    pub fn new(providers: &[(u32, String)], current_slot: Arc<AtomicU64>) -> Arc<Self> {
        let mut registry = Registry::with_prefix("shreds_monitor");

        let received: Family<ProviderLabels, Counter> = Family::default();
        let invalid: Family<ProviderLabels, Counter> = Family::default();
        let duplicate: Family<ProviderLabels, Counter> = Family::default();
        let fec_sets_won: Family<ProviderLabels, Counter> = Family::default();
        let first_shred_delay_ns = Family::<ProviderLabels, Histogram, _>::new_with_constructor(
            || Histogram::new(delay_buckets()),
        );
        registry.register(
            "shreds_received",
            "Shreds received over UDP, per provider",
            received.clone(),
        );
        registry.register(
            "shreds_invalid",
            "Shreds that failed structural/version validation, per provider",
            invalid.clone(),
        );
        registry.register(
            "shreds_duplicate",
            "Duplicate shreds within a FEC set, per provider",
            duplicate.clone(),
        );
        registry.register(
            "fec_sets_won",
            "FEC sets where this provider delivered the first shred",
            fec_sets_won.clone(),
        );
        registry.register(
            "first_shred_delay_ns",
            "First-shred delay vs the baseline provider (negative = earlier)",
            first_shred_delay_ns.clone(),
        );

        let provider_map: HashMap<u32, ProviderMetrics> = providers
            .iter()
            .map(|(id, name)| {
                let labels = ProviderLabels {
                    provider: name.clone(),
                };
                (
                    *id,
                    ProviderMetrics {
                        received: received.get_or_create(&labels).clone(),
                        invalid: invalid.get_or_create(&labels).clone(),
                        duplicate: duplicate.get_or_create(&labels).clone(),
                        fec_sets_won: fec_sets_won.get_or_create(&labels).clone(),
                        first_shred_delay_ns: first_shred_delay_ns
                            .get_or_create(&labels)
                            .clone(),
                    },
                )
            })
            .collect();

        let fec_channel_full: Counter = Counter::default();
        registry.register(
            "fec_channel_full",
            "Shreds dropped because the FEC processing channel was full",
            fec_channel_full.clone(),
        );

        let rows_written: Family<TableLabels, Counter> = Family::default();
        let write_errors: Family<TableLabels, Counter> = Family::default();
        registry.register(
            "clickhouse_rows_written",
            "Rows successfully written to ClickHouse, per table",
            rows_written.clone(),
        );
        registry.register(
            "clickhouse_write_errors",
            "Failed ClickHouse batch writes, per table",
            write_errors.clone(),
        );

        let last_seen_slot: Gauge = Gauge::default();
        registry.register(
            "last_seen_slot",
            "Highest slot observed across all providers",
            last_seen_slot.clone(),
        );

        let uptime_seconds: Gauge<f64, AtomicU64> = Gauge::default();
        registry.register(
            "uptime_seconds",
            "Seconds since the monitor started",
            uptime_seconds.clone(),
        );

        let build_info: Family<BuildLabels, Gauge> = Family::default();
        registry.register(
            "build_info",
            "Constant 1 gauge labeled with the crate version",
            build_info.clone(),
        );
        build_info
            .get_or_create(&BuildLabels {
                version: env!("CARGO_PKG_VERSION").to_string(),
            })
            .set(1);

        Arc::new(Self {
            registry,
            providers: provider_map,
            fec_channel_full,
            rows_written,
            write_errors,
            last_seen_slot,
            uptime_seconds,
            started: Instant::now(),
            current_slot,
        })
    }

    pub fn provider(&self, id: u32) -> Option<&ProviderMetrics> {
        self.providers.get(&id)
    }

    pub fn record_rows_written(&self, table: &str, rows: u64) {
        self.rows_written
            .get_or_create(&TableLabels {
                table: table.to_string(),
            })
            .inc_by(rows);
    }

    pub fn record_write_error(&self, table: &str) {
        self.write_errors
            .get_or_create(&TableLabels {
                table: table.to_string(),
            })
            .inc();
    }

    fn render(&self) -> String {
        self.uptime_seconds.set(self.started.elapsed().as_secs_f64());
        self.last_seen_slot
            .set(self.current_slot.load(Ordering::Relaxed) as i64);
        let mut out = String::new();
        let _ = prometheus_client::encoding::text::encode(&mut out, &self.registry);
        out
    }

    /// Serve `/metrics` on a plain HTTP listener in a background thread.
    pub fn serve(self: Arc<Self>, port: u16) {
        std::thread::spawn(move || {
            let addr = format!("0.0.0.0:{port}");
            let server = match tiny_http::Server::http(&addr) {
                Ok(s) => s,
                Err(e) => {
                    warn!(%addr, error = %e, "metrics server failed to bind");
                    return;
                }
            };
            info!(%addr, "metrics server listening");
            for request in server.incoming_requests() {
                let body = self.render();
                let response = tiny_http::Response::from_string(body).with_header(
                    tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/openmetrics-text; version=1.0.0; charset=utf-8"[..],
                    )
                    .expect("static header"),
                );
                let _ = request.respond(response);
            }
        });
    }
}
