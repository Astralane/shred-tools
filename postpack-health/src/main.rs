mod checker;
mod config;
mod slack;
#[cfg(test)]
mod test_support;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::{error, info};

use checker::Checker;
use config::Config;

#[derive(Parser)]
#[command(name = "postpack-health")]
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

    info!(
        "postpack health: every {}s over a {}s window ending {}s in the past",
        cfg.poll_interval_secs, cfg.window_secs, cfg.window_offset_secs
    );

    let poll = Duration::from_secs(cfg.poll_interval_secs);
    let webhook = cfg.slack_webhook.clone();
    let client = reqwest::Client::new();
    let mut checker = Checker::new(cfg);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(poll) => {}
        }

        match checker.tick().await {
            Ok(Some(alert)) => {
                if let Err(e) = slack::post(&client, &webhook, &alert).await {
                    error!("failed to post alert to slack: {}", e);
                }
            }
            Ok(None) => {}
            Err(e) => error!("postpack degradation query failed: {}", e),
        }
    }
}
