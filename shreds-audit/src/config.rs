use std::net::Ipv4Addr;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// JSON-RPC endpoint used only to fetch the leader schedule.
    pub rpc_url: String,

    /// Every UDP port we bind. A provider entry without `port` is matched on
    /// source IP across all of these.
    pub listen_ports: Vec<u16>,

    #[serde(default = "default_bind_ip")]
    pub bind_ip: Ipv4Addr,

    pub providers: Vec<ProviderCfg>,

    #[serde(default = "default_out_dir")]
    pub output_dir: String,

    /// Seconds of capture per output archive.
    #[serde(default = "default_rotate_secs")]
    pub rotate_secs: u64,

    /// Threads in the verification pool. 0 = number of physical cores minus one.
    #[serde(default)]
    pub verify_threads: usize,

    /// A FEC set is finalized once the highest slot seen has advanced this far
    /// past it.
    #[serde(default = "default_max_wait_slots")]
    pub fec_max_wait_slots: u64,

    /// Drop shreds whose `version` field does not match, if set.
    #[serde(default)]
    pub shred_version: Option<u16>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderCfg {
    pub name: String,
    /// Match only shreds arriving on this port.
    #[serde(default)]
    pub port: Option<u16>,
    /// Match only shreds arriving from these source IPs.
    #[serde(default)]
    pub ips: Vec<Ipv4Addr>,
}

fn default_bind_ip() -> Ipv4Addr {
    Ipv4Addr::UNSPECIFIED
}
fn default_out_dir() -> String {
    "./out".to_string()
}
fn default_rotate_secs() -> u64 {
    600
}
fn default_max_wait_slots() -> u64 {
    10
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = serde_yaml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.providers.is_empty() {
            bail!("config: `providers` is empty");
        }
        if self.listen_ports.is_empty() {
            bail!("config: `listen_ports` is empty");
        }

        for p in &self.providers {
            if p.port.is_none() && p.ips.is_empty() {
                bail!(
                    "provider `{}` has neither `port` nor `ips` — it can never match a packet",
                    p.name
                );
            }
            // A providers pinned to a port we never bind is dead on arrival; fail loudly instead.
            if let Some(port) = p.port {
                if !self.listen_ports.contains(&port) {
                    bail!(
                        "provider `{}` is pinned to port {} which is not in `listen_ports` {:?}",
                        p.name,
                        port,
                        self.listen_ports
                    );
                }
            }
        }

        let mut names: Vec<&str> = self.providers.iter().map(|p| p.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            bail!("config: duplicate provider names");
        }
        Ok(())
    }

    pub fn verify_thread_count(&self) -> usize {
        if self.verify_threads > 0 {
            return self.verify_threads;
        }
        std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1)
    }
}
