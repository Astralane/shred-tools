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

    /// IPv4 multicast groups to receive on — DoubleZero Edge shreds. Each entry
    /// covers one of the `listen_ports`; that port's socket is bound with
    /// `SO_REUSEADDR` and joins the groups as their DoubleZero routes appear.
    /// Empty means no multicast, and the tool behaves exactly as before.
    #[serde(default)]
    pub multicast: Vec<MulticastCfg>,

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

/// One multicast port and the groups to join on it.
///
/// Modelled on raiku-agave's `--multicast-shred-receiver*` flags: the socket
/// binds `0.0.0.0:port` (never a group address, so several groups can share it)
/// and membership is managed separately, gated on the DoubleZero host route.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MulticastCfg {
    /// UDP port the groups deliver to. Must appear in `listen_ports`.
    /// DoubleZero uses 7733 for every group.
    #[serde(default = "default_multicast_port")]
    pub port: u16,

    /// Groups to join. May be combined with `cluster`; the union is used.
    #[serde(default)]
    pub groups: Vec<Ipv4Addr>,

    /// Shorthand for a cluster's leader + turbine-root groups, so the well-known
    /// addresses do not have to be copied into every config.
    #[serde(default)]
    pub cluster: Option<McastCluster>,

    /// IPv4 address of the interface to join on. `0.0.0.0` lets the kernel
    /// routing table pick, which resolves to the DoubleZero interface via the
    /// host route its daemon installs — the same default agave uses.
    #[serde(default = "default_bind_ip")]
    pub interface: Ipv4Addr,

    /// Join a group only while a /32 host route to it exists. On by default
    /// because joining without one succeeds against whatever the default route
    /// names and then receives nothing at all — which reads as DoubleZero
    /// delivering nothing rather than as a tunnel that is down.
    #[serde(default = "default_true")]
    pub require_route: bool,
}

/// Clusters with well-known DoubleZero multicast groups.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McastCluster {
    Mainnet,
    Testnet,
}

impl McastCluster {
    /// `(leader_broadcast, turbine_root)` for this cluster.
    fn groups(self) -> [Ipv4Addr; 2] {
        match self {
            Self::Mainnet => [crate::mcast::MAINNET_LEADER_GROUP, crate::mcast::MAINNET_ROOT_GROUP],
            Self::Testnet => [crate::mcast::TESTNET_LEADER_GROUP, crate::mcast::TESTNET_ROOT_GROUP],
        }
    }
}

impl MulticastCfg {
    /// Every group this entry covers: the cluster's well-known pair plus any
    /// explicit ones, deduplicated and in a stable order.
    pub fn resolved_groups(&self) -> Vec<Ipv4Addr> {
        let mut out: Vec<Ipv4Addr> = Vec::new();
        let from_cluster = self.cluster.map(McastCluster::groups);
        for ip in from_cluster.iter().flatten().chain(self.groups.iter()) {
            if !out.contains(ip) {
                out.push(*ip);
            }
        }
        out
    }
}

fn default_multicast_port() -> u16 {
    crate::mcast::DEFAULT_MULTICAST_SHRED_PORT
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

        // Multicast is validated strictly for the same reason provider rules are:
        // every one of these mistakes ends in a socket that receives nothing,
        // which is indistinguishable in the output from a transport that
        // delivered nothing.
        let mut seen_ports = std::collections::HashSet::new();
        for m in &self.multicast {
            if !self.listen_ports.contains(&m.port) {
                bail!(
                    "multicast port {} is not in `listen_ports` {:?} — no socket would ever be \
                     bound for it",
                    m.port,
                    self.listen_ports
                );
            }
            if !seen_ports.insert(m.port) {
                bail!(
                    "config: two `multicast` entries both claim port {} — one socket is bound per \
                     port, so list every group for it in a single entry",
                    m.port
                );
            }
            let groups = m.resolved_groups();
            if groups.is_empty() {
                bail!(
                    "multicast entry for port {} declares no groups — set `groups`, `cluster`, or \
                     both",
                    m.port
                );
            }
            for g in &groups {
                if !g.is_multicast() {
                    bail!(
                        "multicast group {g} on port {} is not an IPv4 multicast address \
                         (224.0.0.0/4)",
                        m.port
                    );
                }
            }
            // A provider must own the port outright. Matching a multicast port by
            // source IP would mean enumerating every leader that broadcasts on
            // the group, and any leader missed from that list would have its
            // shreds silently counted as unmatched.
            if !self.providers.iter().any(|p| p.port == Some(m.port)) {
                bail!(
                    "no provider is pinned to multicast port {} — add `- name: doublezero` with \
                     `port: {}` so its shreds are attributed instead of counted as unmatched",
                    m.port,
                    m.port
                );
            }
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

    /// The DoubleZero setup from the README, end to end.
    #[test]
    fn a_cluster_shorthand_expands_to_the_leader_and_root_groups() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [7733, 20001]
providers:
  - name: doublezero
    port: 7733
  - name: turbine
    port: 20001
multicast:
  - port: 7733
    cluster: mainnet
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let m = &cfg.multicast[0];
        assert_eq!(
            m.resolved_groups(),
            vec![crate::mcast::MAINNET_LEADER_GROUP, crate::mcast::MAINNET_ROOT_GROUP]
        );
        assert!(m.require_route, "route gating is the safe default");
        assert!(m.interface.is_unspecified(), "let the DZ host route pick the interface");
    }

    /// `cluster` and `groups` compose, and a group named twice is joined once.
    #[test]
    fn explicit_groups_merge_with_the_cluster_shorthand_without_duplicating() {
        let yaml = r#"
rpc_url: http://localhost:8899
listen_ports: [7733]
providers:
  - name: doublezero
    port: 7733
multicast:
  - port: 7733
    cluster: mainnet
    groups: ["233.84.178.1", "233.84.178.99"]
"#;
        let cfg: Config = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        let groups = cfg.multicast[0].resolved_groups();
        assert_eq!(groups.len(), 3, "the repeated leader group is not joined twice");
        assert!(groups.contains(&"233.84.178.99".parse().unwrap()));
    }

    fn multicast_cfg(listen: &str, providers: &str, multicast: &str) -> Result<Config> {
        let yaml = format!(
            r#"
rpc_url: http://localhost:8899
listen_ports: {listen}
providers:
{providers}
multicast:
{multicast}
"#
        );
        let cfg: Config = serde_yaml::from_str(&yaml)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// A multicast port nothing binds receives nothing, and the output would show
    /// that as DoubleZero delivering nothing. Fail at startup instead.
    #[test]
    fn a_multicast_port_outside_listen_ports_is_rejected() {
        let err = multicast_cfg(
            "[20001]",
            "  - name: turbine\n    port: 20001",
            "  - port: 7733\n    cluster: mainnet",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not in `listen_ports`"));
    }

    /// Only one socket is bound per port, so a second entry for it would have its
    /// groups silently dropped.
    #[test]
    fn two_multicast_entries_for_one_port_are_rejected() {
        let err = multicast_cfg(
            "[7733]",
            "  - name: doublezero\n    port: 7733",
            "  - port: 7733\n    groups: [\"233.84.178.1\"]\n  - port: 7733\n    groups: [\"233.84.178.16\"]",
        )
        .unwrap_err();
        assert!(err.to_string().contains("both claim port 7733"));
    }

    #[test]
    fn a_multicast_entry_with_no_groups_is_rejected() {
        let err = multicast_cfg(
            "[7733]",
            "  - name: doublezero\n    port: 7733",
            "  - port: 7733",
        )
        .unwrap_err();
        assert!(err.to_string().contains("declares no groups"));
    }

    /// A unicast address in a multicast block never receives group traffic.
    #[test]
    fn a_non_multicast_group_address_is_rejected() {
        let err = multicast_cfg(
            "[7733]",
            "  - name: doublezero\n    port: 7733",
            "  - port: 7733\n    groups: [\"10.0.0.1\"]",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not an IPv4 multicast address"));
    }

    /// Without a provider pinned to the port, every multicast shred lands in
    /// `udp_unmatched` — received, verified, and then thrown away.
    #[test]
    fn a_multicast_port_with_no_provider_is_rejected() {
        let err = multicast_cfg(
            "[7733, 20001]",
            "  - name: turbine\n    port: 20001",
            "  - port: 7733\n    cluster: mainnet",
        )
        .unwrap_err();
        assert!(err.to_string().contains("no provider is pinned to multicast port 7733"));
    }

    /// The port defaults to DoubleZero's 7733 so it need not be repeated.
    #[test]
    fn the_multicast_port_defaults_to_the_doublezero_shred_port() {
        let cfg = multicast_cfg(
            "[7733]",
            "  - name: doublezero\n    port: 7733",
            "  - cluster: testnet",
        )
        .unwrap();
        assert_eq!(cfg.multicast[0].port, crate::mcast::DEFAULT_MULTICAST_SHRED_PORT);
        assert_eq!(
            cfg.multicast[0].resolved_groups(),
            vec![crate::mcast::TESTNET_LEADER_GROUP, crate::mcast::TESTNET_ROOT_GROUP]
        );
    }

    /// No `multicast` block must leave everything exactly as it was.
    #[test]
    fn multicast_is_absent_by_default() {
        let cfg = parse(
            r#"  - name: legacy
    url: http://localhost:10000"#,
        )
        .unwrap();
        assert!(cfg.multicast.is_empty());
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
