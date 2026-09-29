//! Pings provider IPs: the only place this otherwise passive tool transmits.

use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::{AHashMap, AHashSet};

use crate::config::Config;
use crate::out::{now_unix_ns, ProviderPing};
use crate::registry::{ProviderId, Registry};
use crate::sigreg::SourceKind;

#[derive(Clone, Copy)]
struct PingSample {
    rtt_ms: Option<f64>,
    checked_at_ns: i64,
}

/// Source IPs seen per provider (written by rx threads) and the latest RTT per IP.
#[derive(Default)]
pub struct NetMon {
    observed: Mutex<AHashMap<ProviderId, AHashSet<Ipv4Addr>>>,
    pings: Mutex<AHashMap<Ipv4Addr, PingSample>>,
}

impl NetMon {
    pub fn observe(&self, provider: ProviderId, ip: Ipv4Addr) {
        self.observed
            .lock()
            .unwrap()
            .entry(provider)
            .or_default()
            .insert(ip);
    }

    pub fn provider_pings(&self, cfg: &Config, registry: &Registry) -> Vec<ProviderPing> {
        let observed = self.observed.lock().unwrap().clone();
        let pings = self.pings.lock().unwrap();
        let row = |provider: String, ip: String, kind, source, pinged: Ipv4Addr| {
            let sample = pings.get(&pinged);
            ProviderPing {
                provider,
                ip,
                kind,
                source,
                rtt_ms: sample.and_then(|s| s.rtt_ms),
                checked_at_unix_ns: sample.map(|s| s.checked_at_ns),
            }
        };

        let mut out = Vec::new();
        // ProviderId is the index into cfg.providers.
        for (id, p) in cfg.providers.iter().enumerate() {
            let id = id as ProviderId;
            let mut ips: AHashMap<Ipv4Addr, &'static str> =
                p.ips.iter().map(|ip| (*ip, "configured")).collect();
            for ip in observed.get(&id).into_iter().flatten() {
                ips.entry(*ip).or_insert("observed");
            }
            let mut ips: Vec<_> = ips.into_iter().collect();
            ips.sort_by_key(|(ip, _)| u32::from(*ip));
            for (ip, source) in ips {
                let name = registry.name(id).to_string();
                out.push(row(name, ip.to_string(), SourceKind::Shred, source, ip));
            }
        }
        for g in &cfg.grpc_sources {
            if let Some((host, ip)) = grpc_target(&g.url) {
                out.push(row(g.name.clone(), host, SourceKind::from(g.mode), "configured", ip));
            }
        }
        out
    }

    fn targets(&self, cfg: &Config) -> AHashSet<Ipv4Addr> {
        let mut targets: AHashSet<Ipv4Addr> =
            cfg.providers.iter().flat_map(|p| p.ips.iter().copied()).collect();
        targets.extend(self.observed.lock().unwrap().values().flatten().copied());
        for g in &cfg.grpc_sources {
            targets.extend(grpc_target(&g.url).map(|(_, ip)| ip));
        }
        targets
    }
}

/// Returns the url's host (for display) and the IPv4 it resolves to.
fn grpc_target(url: &str) -> Option<(String, Ipv4Addr)> {
    let rest = url.split("://").last().unwrap_or(url);
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let host = match authority.rsplit_once(':') {
        Some((h, port)) if !h.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => authority,
    }
    .trim();
    if host.is_empty() {
        return None;
    }
    if let Ok(ip) = host.parse::<Ipv4Addr>() {
        return Some((host.to_string(), ip));
    }
    let ip = (host, 0u16).to_socket_addrs().ok()?.find_map(|sa| match sa.ip() {
        IpAddr::V4(v4) => Some(v4),
        IpAddr::V6(_) => None,
    })?;
    Some((host.to_string(), ip))
}

/// Shells out to the system `ping` so no raw-socket privileges are needed.
fn ping_once(ip: Ipv4Addr) -> Option<f64> {
    let out = Command::new("ping")
        .args(["-n", "-c", "3", "-W", "1", &ip.to_string()])
        .output()
        .ok()?;
    parse_avg_rtt(&String::from_utf8_lossy(&out.stdout))
}

/// Parses `rtt min/avg/max/mdev = a/b/c/d ms` (iputils) or `round-trip min/avg/max = ...`.
fn parse_avg_rtt(text: &str) -> Option<f64> {
    let line = text.lines().find(|l| l.contains("min/avg/max"))?;
    let group = line.split('=').nth(1)?.split_whitespace().next()?;
    group.split('/').nth(1)?.parse().ok()
}

pub fn spawn(netmon: Arc<NetMon>, cfg: Config, exit: Arc<AtomicBool>) {
    let period = Duration::from_secs(if cfg.ping_secs == 0 { 30 } else { cfg.ping_secs });
    std::thread::Builder::new()
        .name("pinger".into())
        .spawn(move || {
            // Give the rx threads time to observe source IPs before the first round.
            let mut wait = Duration::from_secs(2);
            while !sleep_interruptible(wait, &exit) {
                for ip in netmon.targets(&cfg) {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    let sample = PingSample {
                        rtt_ms: ping_once(ip),
                        checked_at_ns: now_unix_ns(),
                    };
                    netmon.pings.lock().unwrap().insert(ip, sample);
                }
                wait = period;
            }
        })
        .expect("spawn pinger thread");
}

/// Returns true if `exit` fired.
fn sleep_interruptible(dur: Duration, exit: &AtomicBool) -> bool {
    let step = Duration::from_millis(200);
    let mut waited = Duration::ZERO;
    while waited < dur {
        if exit.load(Ordering::Relaxed) {
            return true;
        }
        std::thread::sleep(step);
        waited += step;
    }
    exit.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_iputils_summary() {
        let s = "rtt min/avg/max/mdev = 0.045/1.234/2.567/0.123 ms";
        assert_eq!(parse_avg_rtt(s), Some(1.234));
    }

    #[test]
    fn parses_roundtrip_summary() {
        let s = "round-trip min/avg/max = 10.1/20.2/30.3 ms";
        assert_eq!(parse_avg_rtt(s), Some(20.2));
    }

    #[test]
    fn grpc_target_parses_ip_host_and_port() {
        let (host, ip) = grpc_target("http://64.130.40.37:10000").unwrap();
        assert_eq!(host, "64.130.40.37");
        assert_eq!(ip, "64.130.40.37".parse::<Ipv4Addr>().unwrap());
        assert_eq!(grpc_target("10.0.0.5").unwrap().0, "10.0.0.5");
        assert_eq!(grpc_target("https://1.2.3.4:443/foo").unwrap().0, "1.2.3.4");
    }

    #[test]
    fn no_summary_line_is_none() {
        assert_eq!(parse_avg_rtt("100% packet loss\n"), None);
    }

    fn ping_cfg() -> Config {
        use crate::config::{GrpcMode, GrpcSourceCfg, ProviderCfg};
        let grpc = |name: &str, url: &str, mode: GrpcMode| GrpcSourceCfg {
            name: name.into(),
            url: url.into(),
            x_token: None,
            mode,
            commitment: None,
            rotate: true,
        };
        Config {
            rpc_url: Some("http://x".into()),
            rpc: None,
            onchain_rpc: None,
            filter_rotation: Default::default(),
            listen_ports: vec![20001],
            bind_ip: Ipv4Addr::UNSPECIFIED,
            providers: vec![ProviderCfg {
                name: "shreds-a".into(),
                port: Some(20001),
                ips: vec!["10.0.0.1".parse().unwrap()],
            }],
            output_dir: "./out".into(),
            export: Default::default(),
            rotate_secs: 600,
            verify_threads: 1,
            fec_max_wait_slots: 10,
            shred_version: None,
            live_secs: 10,
            ping_secs: 30,
            grpc_sources: vec![
                grpc("geyser", "http://10.0.0.2:10000", GrpcMode::Transactions),
                grpc("early", "http://10.0.0.3:10000", GrpcMode::Deshred),
            ],
            onchain_verify: false,
            onchain_sample_secs: 5,
            onchain_rpc_url: None,
            onchain_lag_slots: 32,
        }
    }

    #[test]
    fn ping_rows_separate_feed_kind_from_address_provenance() {
        let cfg = ping_cfg();
        let registry = Registry::build(&cfg);
        let netmon = NetMon::default();
        netmon.observe(0, "10.9.9.9".parse().unwrap());

        let rows = netmon.provider_pings(&cfg, &registry);
        let find = |name: &str| -> Vec<&ProviderPing> {
            rows.iter().filter(|r| r.provider == name).collect()
        };

        let shreds = find("shreds-a");
        assert_eq!(shreds.len(), 2, "one configured ip + one observed");
        assert!(shreds.iter().all(|r| r.kind == SourceKind::Shred));
        let mut provenance: Vec<&str> = shreds.iter().map(|r| r.source).collect();
        provenance.sort_unstable();
        assert_eq!(provenance, ["configured", "observed"]);

        let geyser = find("geyser");
        assert_eq!(geyser.len(), 1);
        assert_eq!(geyser[0].kind, SourceKind::Grpc);
        assert_eq!(geyser[0].source, "configured");

        let early = find("early");
        assert_eq!(early.len(), 1);
        assert_eq!(early[0].kind, SourceKind::GrpcDeshred);
        assert_eq!(early[0].source, "configured");

        assert!(rows.iter().all(|r| r.source == "configured" || r.source == "observed"));
    }

    #[test]
    fn deshred_endpoints_are_ping_targets() {
        let cfg = ping_cfg();
        let targets = NetMon::default().targets(&cfg);
        assert!(targets.contains(&"10.0.0.3".parse().unwrap()));
        assert_eq!(targets.len(), 3, "configured provider ip + two grpc hosts");
    }
}
