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

    /// Audit what each transaction source delivered against the block the cluster
    /// actually produced: sample one recent slot on a timer, fetch its signatures
    /// with `getBlock`, and count every discrepancy. Rides along with the
    /// `grpc_sources` comparison and is inert without it.
    #[serde(default = "default_true")]
    pub onchain_verify: bool,

    /// Seconds between sampled slots — one `getBlock` call each. The audit is a
    /// sample either way (the cluster produces ~2.5 slots/s), so this trades
    /// how fast the rates converge against how hard it leans on the endpoint.
    #[serde(default = "default_onchain_sample_secs")]
    pub onchain_sample_secs: u64,

    /// RPC endpoint for the `getBlock` sampling. Defaults to `rpc_url`. Point it
    /// at your own node if `rpc_url` is a rate-limited public endpoint — the
    /// leader schedule is fetched once an epoch, this is on a timer.
    #[serde(default)]
    pub onchain_rpc_url: Option<String>,

    /// How many slots behind the tip to sample. Far enough back that every source
    /// has had time to deliver the slot and the block is available over RPC.
    #[serde(default = "default_onchain_lag_slots")]
    pub onchain_lag_slots: u64,
}

fn default_txn_settle_secs() -> u64 {
    1
}

fn default_true() -> bool {
    true
}

/// ~13 s behind the tip: past the deshred settle window, past `confirmed`
/// availability, and still a small enough window to hold in memory.
fn default_onchain_lag_slots() -> u64 {
    32
}

/// One block every 5 s — roughly one slot in twelve. Gentle enough that a shared
/// endpoint tolerates it for a long capture, and still ~700 sampled slots an
/// hour, which is far more than the rates need to settle.
fn default_onchain_sample_secs() -> u64 {
    5
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
    /// Which Yellowstone streaming RPC to use. Omitted for legacy configs means
    /// the standard post-execution transaction subscription.
    #[serde(default)]
    pub mode: GrpcMode,
    /// Standard transaction-subscription commitment. Omitted means processed.
    /// SubscribeDeshred has no commitment and rejects this field when present.
    #[serde(default)]
    pub commitment: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcMode {
    #[default]
    Transactions,
    Deshred,
}

impl GrpcSourceCfg {
    pub fn effective_commitment(&self) -> &str {
        self.commitment.as_deref().unwrap_or("processed")
    }
}

impl Config {
    /// Endpoint the onchain audit samples blocks from.
    pub fn effective_onchain_rpc_url(&self) -> &str {
        self.onchain_rpc_url.as_deref().unwrap_or(&self.rpc_url)
    }
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
            match g.mode {
                GrpcMode::Transactions => match g.effective_commitment().to_lowercase().as_str() {
                    "processed" | "confirmed" | "finalized" => {}
                    other => bail!("grpc source `{}`: invalid commitment `{other}`", g.name),
                },
                GrpcMode::Deshred if g.commitment.is_some() => {
                    bail!(
                        "grpc source `{}`: `commitment` is not supported in deshred mode",
                        g.name
                    )
                }
                GrpcMode::Deshred => {}
            }
        }

        if self.onchain_verify {
            if let Some(url) = &self.onchain_rpc_url {
                if url.trim().is_empty() {
                    bail!("config: `onchain_rpc_url` is set but empty");
                }
            }
            // Not clamped to a default: an interval of zero is a request to call
            // getBlock in a tight loop, and silently rewriting that to something
            // else hides a misconfiguration behind an endpoint's rate limiter.
            if self.onchain_sample_secs == 0 {
                bail!(
                    "config: `onchain_sample_secs` is 0 — that samples getBlock in a tight loop; \
                     set the seconds between sampled slots (default 5), or `onchain_verify: false`"
                );
            }
            // Sampling too close to the tip audits slots the sources have not
            // finished delivering and blames them for our own impatience, which
            // is worse than not auditing at all.
            if self.onchain_lag_slots < 8 {
                bail!(
                    "config: `onchain_lag_slots` is {} — sampling that close to the tip audits \
                     slots before every source has delivered them and reports the shortfall as \
                     missed transactions; use 8 or more (default 32)",
                    self.onchain_lag_slots
                );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(grpc_source: &str) -> Result<Config> {
        let yaml = format!(
            r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
grpc_sources:
{grpc_source}
"#
        );
        let cfg: Config = serde_yaml::from_str(&yaml)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn legacy_grpc_source_defaults_to_transactions_at_processed() {
        let cfg = parse(
            r#"  - name: legacy
    url: http://localhost:10000"#,
        )
        .unwrap();
        let source = &cfg.grpc_sources[0];
        assert_eq!(source.mode, GrpcMode::Transactions);
        assert_eq!(source.commitment, None);
        assert_eq!(source.effective_commitment(), "processed");
    }

    #[test]
    fn deshred_source_accepts_no_commitment() {
        let cfg = parse(
            r#"  - name: early
    url: http://localhost:10000
    mode: deshred"#,
        )
        .unwrap();
        assert_eq!(cfg.grpc_sources[0].mode, GrpcMode::Deshred);
    }

    #[test]
    fn deshred_source_rejects_commitment() {
        let err = parse(
            r#"  - name: early
    url: http://localhost:10000
    mode: deshred
    commitment: processed"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not supported in deshred mode"));
    }

    #[test]
    fn onchain_audit_is_on_by_default_and_falls_back_to_rpc_url() {
        let cfg = parse(
            r#"  - name: early
    url: http://localhost:10000
    mode: deshred"#,
        )
        .unwrap();
        assert!(cfg.onchain_verify);
        assert_eq!(cfg.onchain_lag_slots, 32);
        assert_eq!(cfg.onchain_sample_secs, 5);
        assert_eq!(cfg.effective_onchain_rpc_url(), "http://localhost:8899");
    }

    #[test]
    fn a_sampling_interval_can_be_set() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
onchain_sample_secs: 30
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.onchain_sample_secs, 30);
    }

    #[test]
    fn a_zero_sampling_interval_is_rejected() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
onchain_sample_secs: 0
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("onchain_sample_secs"));
    }

    #[test]
    fn sampling_too_close_to_the_tip_is_rejected() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
onchain_lag_slots: 2
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("onchain_lag_slots"));
    }

    #[test]
    fn a_disabled_audit_does_not_police_its_own_settings() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
onchain_verify: false
onchain_lag_slots: 2
onchain_sample_secs: 0
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn a_dedicated_onchain_endpoint_overrides_rpc_url() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [20001]
providers:
  - name: shreds
    port: 20001
onchain_rpc_url: http://my-node:8899
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.effective_onchain_rpc_url(), "http://my-node:8899");
    }

    #[test]
    fn unknown_grpc_mode_is_rejected() {
        let err = parse(
            r#"  - name: broken
    url: http://localhost:10000
    mode: unknown"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown variant"));
    }
}
