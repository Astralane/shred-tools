use std::{collections::HashSet, path::Path};

use anyhow::{bail, Result};
use serde::Deserialize;

use crate::leader_schedule::LeaderScheduleConfig;

#[derive(Deserialize)]
pub struct Config {
    pub clickhouse: ClickhouseConfig,
    pub shred_sources: Vec<ShredSourceConfig>,
}

#[derive(Deserialize)]
pub struct ShredSourceConfig {
    pub name: String,
    pub udp_port: u16,
    pub provider_id: u32,
    #[serde(default)]
    pub baseline: bool,
}

#[derive(Deserialize)]
pub struct ClickhouseConfig {
    pub url: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default = "default_password")]
    pub password: String,
    #[serde(default)]
    pub shred_version: Option<u16>,
    #[serde(default)]
    pub baseline_provider_id: u32,
    #[serde(default = "default_fec_grace_ms")]
    pub fec_grace_ms: u64,
    #[serde(default = "default_fec_max_wait_slots")]
    pub fec_max_wait_slots: u64,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_fec_table")]
    pub table: String,
    #[serde(default)]
    pub leader_schedule: LeaderScheduleConfig,
}

fn default_database() -> String {
    "default".to_string()
}
fn default_user() -> String {
    "default".to_string()
}
fn default_password() -> String {
    "default".to_string()
}
fn default_fec_grace_ms() -> u64 {
    3_000
}
fn default_fec_max_wait_slots() -> u64 {
    10
}
fn default_flush_interval_ms() -> u64 {
    1_000
}
fn default_batch_rows() -> usize {
    100_000
}
fn default_fec_table() -> String {
    "fec_stats".to_string()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        let url = &self.clickhouse.url;
        if !url.starts_with("http://") && !url.starts_with("https://") {
            bail!("clickhouse.url must start with http:// or https://");
        }
        if self.shred_sources.is_empty() {
            bail!("config must have at least 1 shred source");
        }
        let mut names = HashSet::new();
        let mut ports = HashSet::new();
        let mut ids = HashSet::new();
        for source in &self.shred_sources {
            if !names.insert(&source.name) {
                bail!("duplicate source name: {}", source.name);
            }
            if !ports.insert(source.udp_port) {
                bail!("duplicate udp_port: {}", source.udp_port);
            }
            if source.provider_id == 0 {
                bail!("source {} needs a non-zero provider_id", source.name);
            }
            if !ids.insert(source.provider_id) {
                bail!("duplicate provider_id: {}", source.provider_id);
            }
        }
        Ok(())
    }

    pub fn baseline_provider_id(&self) -> u32 {
        if self.clickhouse.baseline_provider_id != 0 {
            return self.clickhouse.baseline_provider_id;
        }
        self.shred_sources
            .iter()
            .find(|s| s.baseline)
            .map(|s| s.provider_id)
            .unwrap_or(0)
    }

    pub fn provider_rows(&self) -> Vec<(u32, String)> {
        self.shred_sources
            .iter()
            .map(|s| (s.provider_id, s.name.clone()))
            .collect()
    }
}
