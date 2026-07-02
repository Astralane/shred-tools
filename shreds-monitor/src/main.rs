mod clickhouse;
mod config;
mod fec_stats;
mod leader_schedule;
mod metrics;
mod receiver;

use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use config::Config;
use fec_stats::{FecShred, FecStatsParams};
use leader_schedule::LeaderSchedule;
use metrics::Metrics;
use receiver::{ShredPacket, UdpReceiver};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const FEC_CHANNEL_CAPACITY: usize = 500_000;

#[derive(Parser)]
#[command(name = "shreds-monitor")]
struct Cli {
    #[arg(long)]
    config_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config_path)?;

    let cancel = CancellationToken::new();
    let current_slot = Arc::new(AtomicU64::new(0));
    let schedule = LeaderSchedule::new();

    let metrics = Metrics::new(&cfg.provider_rows(), Arc::clone(&current_slot));
    if cfg.metrics_port != 0 {
        Arc::clone(&metrics).serve(cfg.metrics_port);
    }

    let client = clickhouse::create_clickhouse_client(&cfg.clickhouse);
    if let Err(e) = fec_stats::upsert_providers(&client, &cfg.provider_rows()).await {
        tracing::warn!(error = %e, "failed to upsert providers table (dashboard names may be missing)");
    }

    tokio::spawn(leader_schedule::run(
        cfg.clickhouse.leader_schedule.clone(),
        clickhouse::create_clickhouse_client(&cfg.clickhouse),
        Arc::clone(&schedule),
        Arc::clone(&current_slot),
        cancel.clone(),
    ));

    let (fec_tx, fec_rx) = mpsc::channel::<FecShred>(FEC_CHANNEL_CAPACITY);
    let params = FecStatsParams {
        client,
        table: cfg.clickhouse.table.clone(),
        baseline_provider_id: cfg.baseline_provider_id(),
        shred_version: cfg.clickhouse.shred_version,
        fec_grace_ms: cfg.clickhouse.fec_grace_ms,
        fec_max_wait_slots: cfg.clickhouse.fec_max_wait_slots,
        flush_interval_ms: cfg.clickhouse.flush_interval_ms,
        batch_rows: cfg.clickhouse.batch_rows,
        metrics: Arc::clone(&metrics),
    };
    let fec_handle = tokio::spawn(fec_stats::run(
        params,
        fec_rx,
        Arc::clone(&current_slot),
        Arc::clone(&schedule),
        cancel.clone(),
    ));

    let mut recv_handles = Vec::new();
    for source in &cfg.shred_sources {
        let (tx, mut rx) = mpsc::unbounded_channel::<ShredPacket>();
        let recv = UdpReceiver {
            source_name: source.name.clone(),
            udp_port: source.udp_port,
        };
        let cancel_clone = cancel.clone();
        recv_handles.push(tokio::task::spawn_blocking(move || {
            recv.run(tx, cancel_clone);
        }));

        let fec_tx = fec_tx.clone();
        let provider_id = source.provider_id;
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            while let Some(pkt) = rx.recv().await {
                if let Some(p) = metrics.provider(provider_id) {
                    p.received.inc();
                }
                let msg = FecShred {
                    provider_id,
                    rx: pkt.received_at,
                    shred: Bytes::from(pkt.data),
                };
                if fec_tx.try_send(msg).is_err() {
                    metrics.fec_channel_full.inc();
                }
            }
        });
    }
    drop(fec_tx);

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        cancel_clone.cancel();
    });

    cancel.cancelled().await;

    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), fec_handle).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        for h in recv_handles {
            let _ = h.await;
        }
    })
    .await;

    Ok(())
}
