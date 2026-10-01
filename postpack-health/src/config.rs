use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub clickhouse: ClickhouseConfig,
    pub slack_webhook: String,
    #[serde(default)]
    pub shred_host: Option<String>,
    #[serde(default)]
    pub endpoints: Vec<String>,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,
    #[serde(default = "default_window_offset_secs")]
    pub window_offset_secs: u64,
    #[serde(default = "default_min_pairs")]
    pub min_pairs: u64,
    #[serde(default = "default_win_rate_threshold")]
    pub win_rate_threshold: f64,
    #[serde(default = "default_recovery_win_rate")]
    pub recovery_win_rate: f64,
    #[serde(default = "default_late_ms_threshold")]
    pub late_ms_threshold: u64,
    #[serde(default = "default_late_rate_threshold")]
    pub late_rate_threshold: f64,
    #[serde(default = "default_repeat_alert_secs")]
    pub repeat_alert_secs: u64,
    #[serde(default = "default_no_data_alert_after_secs")]
    pub no_data_alert_after_secs: u64,
    #[serde(default = "default_land_lookahead_secs")]
    pub land_lookahead_secs: u64,
    #[serde(default = "default_query_timeout_secs")]
    pub query_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickhouseConfig {
    pub url: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_database")]
    pub database: String,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config from {}", path.display()))?;
        serde_yaml::from_str(&raw).context("parsing config")
    }
}

fn default_user() -> String {
    "default".to_string()
}

fn default_database() -> String {
    "shred_indexer".to_string()
}

fn default_poll_interval_secs() -> u64 {
    60
}

fn default_window_secs() -> u64 {
    900
}

fn default_window_offset_secs() -> u64 {
    300
}

fn default_min_pairs() -> u64 {
    100
}

fn default_win_rate_threshold() -> f64 {
    0.85
}

fn default_recovery_win_rate() -> f64 {
    0.90
}

fn default_late_ms_threshold() -> u64 {
    100
}

fn default_late_rate_threshold() -> f64 {
    0.10
}

fn default_repeat_alert_secs() -> u64 {
    900
}

fn default_no_data_alert_after_secs() -> u64 {
    1800
}

fn default_land_lookahead_secs() -> u64 {
    120
}

fn default_query_timeout_secs() -> u64 {
    60
}
