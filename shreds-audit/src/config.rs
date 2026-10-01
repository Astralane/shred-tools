use std::collections::HashSet;
use std::net::Ipv4Addr;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::filters::bundles::{self, BundleCfg};
use crate::rpc::{RpcCfg, RpcEndpoint};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub rpc_url: Option<String>,

    #[serde(default)]
    pub rpc: Option<RpcCfg>,

    /// A provider without `port` is matched on source IP across all of these.
    #[serde(default)]
    pub listen_ports: Vec<u16>,

    #[serde(default = "default_bind_ip")]
    pub bind_ip: Ipv4Addr,

    #[serde(default)]
    pub providers: Vec<ProviderCfg>,

    #[serde(default = "default_out_dir")]
    pub output_dir: String,

    #[serde(default)]
    pub export: ExportCfg,

    #[serde(default = "default_rotate_secs")]
    pub rotate_secs: u64,

    /// 0 = number of cores minus one.
    #[serde(default)]
    pub verify_threads: usize,

    #[serde(default = "default_max_wait_slots")]
    pub fec_max_wait_slots: u64,

    #[serde(default)]
    pub shred_version: Option<u16>,

    /// 0 falls back to 10.
    #[serde(default = "default_live_secs")]
    pub live_secs: u64,

    /// 0 falls back to 30.
    #[serde(default = "default_ping_secs")]
    pub ping_secs: u64,

    #[serde(default)]
    pub grpc_sources: Vec<GrpcSourceCfg>,

    #[serde(default = "default_true")]
    pub onchain_verify: bool,

    #[serde(default = "default_onchain_sample_secs")]
    pub onchain_sample_secs: u64,

    #[serde(default)]
    pub onchain_rpc_url: Option<String>,

    #[serde(default)]
    pub onchain_rpc: Option<RpcCfg>,

    #[serde(default)]
    pub filter_rotation: FilterRotationCfg,

    #[serde(default = "default_onchain_lag_slots")]
    pub onchain_lag_slots: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ExportCfg {
    #[serde(default)]
    pub mode: ExportMode,
    #[serde(default)]
    pub postgres: PostgresCfg,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresCfg {
    #[serde(default)]
    pub url: String,

    #[serde(default = "default_flush_secs")]
    pub flush_secs: u64,
}

impl Default for PostgresCfg {
    fn default() -> Self {
        Self {
            url: String::new(),
            flush_secs: default_flush_secs(),
        }
    }
}

#[derive(
    Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub enum ExportMode {
    #[default]
    Zip,
    Postgres,
    Off,
}

impl ExportMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Zip => "zip",
            Self::Postgres => "postgres",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcSourceCfg {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub x_token: Option<String>,
    #[serde(default)]
    pub mode: GrpcMode,
    /// Transactions mode only; omitted means processed.
    #[serde(default)]
    pub commitment: Option<String>,
    #[serde(default = "default_true")]
    pub rotate: bool,
}

impl GrpcSourceCfg {
    pub fn effective_commitment(&self) -> &str {
        self.commitment.as_deref().unwrap_or("processed")
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcMode {
    #[default]
    Transactions,
    Deshred,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilterRotationCfg {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_rotation_min_secs")]
    pub min_secs: u64,
    #[serde(default = "default_rotation_max_secs")]
    pub max_secs: u64,
    #[serde(default = "default_sample_every_slots")]
    pub sample_every_slots: u64,
    #[serde(default)]
    pub bundles: Option<Vec<BundleCfg>>,
}

impl Default for FilterRotationCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            min_secs: default_rotation_min_secs(),
            max_secs: default_rotation_max_secs(),
            sample_every_slots: default_sample_every_slots(),
            bundles: None,
        }
    }
}

impl FilterRotationCfg {
    pub fn effective_bundles(&self) -> Vec<BundleCfg> {
        self.bundles.clone().unwrap_or_else(bundles::defaults)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderCfg {
    pub name: String,
    #[serde(default)]
    pub port: Option<u16>,
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
fn default_live_secs() -> u64 {
    10
}
fn default_ping_secs() -> u64 {
    30
}
fn default_true() -> bool {
    true
}
fn default_onchain_sample_secs() -> u64 {
    5
}
fn default_onchain_lag_slots() -> u64 {
    32
}
fn default_flush_secs() -> u64 {
    15
}
fn default_rotation_min_secs() -> u64 {
    30
}
fn default_rotation_max_secs() -> u64 {
    60
}
fn default_sample_every_slots() -> u64 {
    10
}

/// `key` and `key_url` are alternative spellings of one endpoint; at most one may be set.
fn endpoint(rpc: &Option<RpcCfg>, url: &Option<String>, key: &str) -> Result<Option<RpcEndpoint>> {
    match (rpc, url) {
        (Some(_), Some(_)) => bail!("config: set `{key}` or `{key}_url`, not both"),
        (Some(r), None) => r.resolve().map(Some),
        (None, Some(u)) => RpcCfg::generic(u).resolve().map(Some),
        (None, None) => Ok(None),
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let mut cfg: Config = serde_yaml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn rpc_endpoint(&self) -> Result<RpcEndpoint> {
        match endpoint(&self.rpc, &self.rpc_url, "rpc")? {
            Some(e) => Ok(e),
            None => bail!("config: no RPC endpoint — set `rpc` or `rpc_url`"),
        }
    }

    /// Endpoint the onchain audit samples blocks from; defaults to the main one.
    pub fn onchain_rpc_endpoint(&self) -> Result<RpcEndpoint> {
        match endpoint(&self.onchain_rpc, &self.onchain_rpc_url, "onchain_rpc")? {
            Some(e) => Ok(e),
            None => self.rpc_endpoint(),
        }
    }

    pub fn rotating(&self, g: &GrpcSourceCfg) -> bool {
        self.filter_rotation.enabled && g.rotate
    }

    pub fn verify_thread_count(&self) -> usize {
        if self.verify_threads > 0 {
            return self.verify_threads;
        }
        std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1)
    }

    pub fn validate(&mut self) -> Result<()> {
        self.rpc_endpoint()?;
        if self.onchain_rpc_url.as_ref().is_some_and(|u| u.trim().is_empty()) {
            bail!("config: `onchain_rpc_url` is set but empty");
        }
        self.onchain_rpc_endpoint()?;

        if self.providers.is_empty() && self.grpc_sources.is_empty() {
            bail!("config: `providers` and `grpc_sources` are empty");
        }

        if self.listen_ports.is_empty() {
            for port in self.providers.iter().filter_map(|p| p.port) {
                if !self.listen_ports.contains(&port) {
                    self.listen_ports.push(port);
                }
            }
        }

        if !self.providers.is_empty() && self.listen_ports.is_empty() {
            bail!("config: `providers` is configured but `listen_ports` is empty and cannot be deduced");
        }

        for p in &self.providers {
            if p.port.is_none() && p.ips.is_empty() {
                bail!(
                    "provider `{}` has neither `port` nor `ips` — it can never match a packet",
                    p.name
                );
            }
            if let Some(port) = p.port.filter(|port| !self.listen_ports.contains(port)) {
                bail!(
                    "provider `{}` is pinned to port {} which is not in `listen_ports` {:?}",
                    p.name,
                    port,
                    self.listen_ports
                );
            }
        }
        let mut names = HashSet::new();
        if !self.providers.iter().all(|p| names.insert(p.name.as_str())) {
            bail!("config: duplicate provider names");
        }

        let mut grpc_names = HashSet::new();
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

        let fr = &self.filter_rotation;
        if fr.enabled && self.grpc_sources.iter().any(|g| g.rotate) {
            if fr.min_secs < 10 || fr.max_secs < fr.min_secs {
                bail!(
                    "config: `filter_rotation` needs 10 <= min_secs <= max_secs (got {}..{}) — a \
                     window shorter than that leaves too few slots to audit",
                    fr.min_secs,
                    fr.max_secs
                );
            }
            if fr.sample_every_slots == 0 {
                bail!("config: `filter_rotation.sample_every_slots` must be at least 1");
            }
            let rotating_mode =
                |mode| self.grpc_sources.iter().any(|g| g.rotate && g.mode == mode);
            bundles::validate(
                &fr.effective_bundles(),
                rotating_mode(GrpcMode::Deshred),
                rotating_mode(GrpcMode::Transactions),
            )?;
        }

        if self.onchain_verify {
            if self.onchain_sample_secs == 0 {
                bail!(
                    "config: `onchain_sample_secs` is 0 — that samples getBlock in a tight loop; \
                     set the seconds between sampled slots (default 5), or `onchain_verify: false`"
                );
            }
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
        let mut cfg: Config = serde_yaml::from_str(&yaml)?;
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
        assert_eq!(
            cfg.onchain_rpc_endpoint().unwrap().label(),
            "generic http://localhost:8899"
        );
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
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
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
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
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
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
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
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
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
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.onchain_rpc_endpoint().unwrap().label(),
            "generic http://my-node:8899"
        );
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

    #[test]
    fn grpc_only_runs_no_shred_providers_is_allowed() {
        let yaml = r#"
            rpc_url: http://localhost:8899
            grpc_sources:
              - name: my-grpc
                url: http://localhost:10000
        "#;
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.providers.is_empty());
        assert!(cfg.listen_ports.is_empty());
    }

    #[test]
    fn empty_providers_and_empty_grpc_sources_is_rejected() {
        let yaml = r#"
            rpc_url: http://localhost:8899
        "#;
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("are empty"));
    }

    #[test]
    fn providers_with_port_auto_infers_listen_ports() {
        let yaml = r#"
            rpc_url: http://localhost:8899
            providers:
              - name: shreds1
                port: 20001
              - name: shreds2
                port: 20002
              - name: shreds3
                port: 20001
        "#;
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.listen_ports, vec![20001, 20002]);
    }

    #[test]
    fn ip_only_providers_without_listen_ports_is_rejected() {
        let yaml = r#"
            rpc_url: http://localhost:8899
            providers:
              - name: shreds
                ips: [127.0.0.1]
        "#;
        let mut cfg: Config = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("listen_ports` is empty and cannot be deduced"));
    }

    fn parse_full(yaml: &str) -> Result<Config> {
        let mut cfg: Config = serde_yaml::from_str(yaml)?;
        cfg.validate()?;
        Ok(cfg)
    }

    const DESHRED_SOURCE: &str = r#"
grpc_sources:
  - name: hub
    url: http://hub:10000
    mode: deshred
"#;

    #[test]
    fn switching_rpc_provider_keeps_the_same_fields() {
        std::env::set_var("SHRED_AUDIT_TEST_SHYFT", "k");
        let cfg = parse_full(&format!(
            "rpc:\n  provider: shyft\n  token: env:SHRED_AUDIT_TEST_SHYFT\n{DESHRED_SOURCE}"
        ))
        .unwrap();
        assert_eq!(
            cfg.rpc_endpoint().unwrap().label(),
            "shyft https://rpc.shyft.to"
        );
        assert_eq!(
            cfg.onchain_rpc_endpoint().unwrap().label(),
            "shyft https://rpc.shyft.to",
            "the audits default to the main endpoint"
        );
        let cfg = parse_full(&format!(
            "rpc:\n  provider: helius\n  token: env:SHRED_AUDIT_TEST_SHYFT\n{DESHRED_SOURCE}"
        ))
        .unwrap();
        assert_eq!(
            cfg.rpc_endpoint().unwrap().label(),
            "helius https://mainnet.helius-rpc.com"
        );
    }

    #[test]
    fn rpc_and_rpc_url_are_mutually_exclusive() {
        let err = parse_full(&format!(
            "rpc_url: http://a:8899\nrpc:\n  url: http://b:8899\n{DESHRED_SOURCE}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("not both"));
        assert!(parse_full(DESHRED_SOURCE).is_err(), "no endpoint at all");
    }

    #[test]
    fn a_missing_provider_token_fails_at_startup() {
        let err = parse_full(&format!("rpc:\n  provider: shyft\n{DESHRED_SOURCE}")).unwrap_err();
        assert!(err.to_string().contains("token"));
    }

    #[test]
    fn rotation_is_on_by_default_and_per_source_opt_out_works() {
        let cfg = parse_full(&format!(
            "rpc_url: http://a:8899\n{DESHRED_SOURCE}  - name: baseline\n    url: http://b:1\n    rotate: false\n"
        ))
        .unwrap();
        assert!(cfg.filter_rotation.enabled);
        assert_eq!(cfg.filter_rotation.sample_every_slots, 10);
        assert!(cfg.rotating(&cfg.grpc_sources[0]));
        assert!(!cfg.rotating(&cfg.grpc_sources[1]));
    }

    #[test]
    fn rotation_windows_must_be_long_enough_to_audit() {
        let err = parse_full(&format!(
            "rpc_url: http://a:8899\nfilter_rotation:\n  min_secs: 5\n  max_secs: 6\n{DESHRED_SOURCE}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("min_secs"));
    }

    #[test]
    fn custom_bundles_are_validated_against_the_source_modes() {
        let err = parse_full(&format!(
            "rpc_url: http://a:8899\nfilter_rotation:\n  bundles:\n    - name: f\n      filters:\n        \
             failed_only: {{failed: true}}\n{DESHRED_SOURCE}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("deshred"));
        let ok = parse_full(&format!(
            "rpc_url: http://a:8899\nfilter_rotation:\n  bundles:\n    - name: mine\n      filters:\n        \
             jup: {{vote: false, account_include: [JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4]}}\n{DESHRED_SOURCE}"
        ))
        .unwrap();
        assert_eq!(ok.filter_rotation.effective_bundles()[0].name, "mine");
    }

    #[test]
    fn the_example_config_is_valid() {
        let cfg = Config::load(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.yaml")).unwrap();
        assert_eq!(cfg.providers.len(), 3);
        assert_eq!(cfg.grpc_sources.len(), 3);
    }
}
