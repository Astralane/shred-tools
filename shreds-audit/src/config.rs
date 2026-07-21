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

    /// With `--live`, how often (seconds) to refresh the stable `live.zip`
    /// snapshot of the current window. 0 falls back to 10.
    #[serde(default = "default_live_secs")]
    pub live_secs: u64,

    /// How often (seconds) to re-ping each provider's IPs. 0 falls back to 30.
    #[serde(default = "default_ping_secs")]
    pub ping_secs: u64,

    /// Optional Geyser/Yellowstone gRPC feeds to compare against the shred stream
    /// by transaction arrival time. When empty, the transaction-timing comparison
    /// is entirely inert and the tool behaves exactly as before.
    #[serde(default)]
    pub grpc_sources: Vec<GrpcSourceCfg>,

    /// Seconds a slot must be quiet (no new shred) before its buffered shreds are
    /// reconstructed into transactions for the timing comparison. 0 falls back to 1.
    #[serde(default = "default_txn_settle_secs")]
    pub txn_settle_secs: u64,
}

fn default_txn_settle_secs() -> u64 {
    1
}

fn default_commitment() -> String {
    "processed".to_string()
}

/// A Geyser gRPC transaction feed for the shred-vs-gRPC timing comparison.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcSourceCfg {
    pub name: String,
    /// gRPC endpoint, e.g. `https://host:port`.
    pub url: String,
    /// Optional `x-token` auth header value.
    #[serde(default)]
    pub x_token: Option<String>,
    /// Subscription commitment: processed | confirmed | finalized. Default processed.
    #[serde(default = "default_commitment")]
    pub commitment: String,
}

fn default_live_secs() -> u64 {
    10
}

fn default_ping_secs() -> u64 {
    30
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

        let mut grpc_names = std::collections::HashSet::new();
        for g in &self.grpc_sources {
            if g.url.trim().is_empty() {
                bail!("grpc source `{}` has an empty url", g.name);
            }
            if !grpc_names.insert(g.name.as_str()) {
                bail!("config: duplicate grpc source name `{}`", g.name);
            }
            match g.commitment.to_lowercase().as_str() {
                "processed" | "confirmed" | "finalized" => {}
                other => bail!("grpc source `{}`: invalid commitment `{other}`", g.name),
            }
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
