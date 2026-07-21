//! Active reachability probing — the one place this tool transmits.
//!
//! The rest of shred-audit is strictly passive. Pings only answer "which hosts
//! is each provider sending from, and roughly how far away?" — a coarse
//! reachability/RTT hint, NOT comparable to the decode deltas (ICMP can be
//! routed differently, rate-limited, or deprioritized).
//!
//! For an IP-configured provider we ping the configured IPs; for a
//! port-configured one, every source IP observed on the wire by the rx threads.

use std::net::Ipv4Addr;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::{AHashMap, AHashSet};

use crate::config::Config;
use crate::out::{now_unix_ns, ProviderPing};
use crate::registry::{ProviderId, Registry};

/// Latest ping result for one IP.
#[derive(Clone)]
struct PingSample {
    /// Average RTT in milliseconds, or `None` if the host did not reply.
    rtt_ms: Option<f64>,
    checked_at_ns: i64,
}

/// Shared record of which source IPs each provider sends from, plus the latest
/// ping RTT for each IP. Written by the rx threads (observed IPs) and the pinger
/// thread (RTTs); read whenever a manifest is built.
#[derive(Default)]
pub struct NetMon {
    observed: Mutex<AHashMap<ProviderId, AHashSet<Ipv4Addr>>>,
    pings: Mutex<AHashMap<Ipv4Addr, PingSample>>,
}

impl NetMon {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `ip` was seen sending to `provider`. The rx loop dedups per
    /// thread, so this locks only on the first sight of each `(provider, ip)`.
    pub fn observe(&self, provider: ProviderId, ip: Ipv4Addr) {
        self.observed
            .lock()
            .unwrap()
            .entry(provider)
            .or_default()
            .insert(ip);
    }

    fn observed_snapshot(&self) -> AHashMap<ProviderId, Vec<Ipv4Addr>> {
        self.observed
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.iter().copied().collect()))
            .collect()
    }

    fn record_ping(&self, ip: Ipv4Addr, sample: PingSample) {
        self.pings.lock().unwrap().insert(ip, sample);
    }

    fn ping_of(&self, ip: &Ipv4Addr) -> Option<PingSample> {
        self.pings.lock().unwrap().get(ip).cloned()
    }

    /// Per-(provider, ip) rows for the manifest. Combines the IPs declared in
    /// the config with the IPs observed on the wire, and attaches the latest RTT.
    pub fn provider_pings(&self, cfg: &Config, registry: &Registry) -> Vec<ProviderPing> {
        let observed = self.observed_snapshot();
        let mut out = Vec::new();
        // Registry assigns ProviderId = index into cfg.providers, so enumerate.
        for (idx, p) in cfg.providers.iter().enumerate() {
            let id = idx as ProviderId;
            // "configured" wins over "observed" if an IP is both.
            let mut ips: AHashMap<Ipv4Addr, &'static str> = AHashMap::new();
            for ip in &p.ips {
                ips.insert(*ip, "configured");
            }
            if let Some(seen) = observed.get(&id) {
                for ip in seen {
                    ips.entry(*ip).or_insert("observed");
                }
            }
            let mut ips: Vec<(Ipv4Addr, &'static str)> = ips.into_iter().collect();
            ips.sort_by_key(|(ip, _)| u32::from(*ip));
            for (ip, source) in ips {
                let sample = self.ping_of(&ip);
                out.push(ProviderPing {
                    provider: registry.name(id).to_string(),
                    ip: ip.to_string(),
                    source,
                    rtt_ms: sample.as_ref().and_then(|s| s.rtt_ms),
                    checked_at_unix_ns: sample.as_ref().map(|s| s.checked_at_ns),
                });
            }
        }
        // gRPC sources: ping the host from each url, shown alongside the providers.
        for g in &cfg.grpc_sources {
            if let Some((host, ip)) = grpc_target(&g.url) {
                let sample = self.ping_of(&ip);
                out.push(ProviderPing {
                    provider: g.name.clone(),
                    ip: host,
                    source: "grpc",
                    rtt_ms: sample.as_ref().and_then(|s| s.rtt_ms),
                    checked_at_unix_ns: sample.as_ref().map(|s| s.checked_at_ns),
                });
            }
        }
        out
    }
}

/// Extract the host from a gRPC url and resolve it to an IPv4 to ping. Returns
/// the host string (for display) and the address actually pinged.
fn grpc_target(url: &str) -> Option<(String, Ipv4Addr)> {
    let rest = url.split("://").last().unwrap_or(url);
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    // Strip a trailing :port (rightmost numeric colon); hosts are IPv4 or DNS names.
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
    use std::net::ToSocketAddrs;
    let ip = (host, 0u16)
        .to_socket_addrs()
        .ok()?
        .find_map(|sa| match sa.ip() {
            std::net::IpAddr::V4(v4) => Some(v4),
            _ => None,
        })?;
    Some((host.to_string(), ip))
}

/// Ping one host by shelling out to the system `ping` binary. Unprivileged, no
/// raw sockets. Returns the average RTT in ms, or `None` on no reply / error.
fn ping_once(ip: Ipv4Addr, count: u32, timeout_s: u32) -> Option<f64> {
    let out = Command::new("ping")
        .args([
            "-n", // numeric, no reverse DNS
            "-c",
            &count.to_string(),
            "-W",
            &timeout_s.to_string(),
            &ip.to_string(),
        ])
        .output()
        .ok()?;
    parse_avg_rtt(&String::from_utf8_lossy(&out.stdout))
}

/// Pull the average RTT out of `ping`'s summary line. iputils prints
/// `rtt min/avg/max/mdev = 0.123/0.456/0.789/0.012 ms`; some builds say
/// `round-trip min/avg/max = ...`. Either way the average is the 2nd field.
fn parse_avg_rtt(text: &str) -> Option<f64> {
    let line = text.lines().find(|l| l.contains("min/avg/max"))?;
    let after_eq = line.split('=').nth(1)?;
    let group = after_eq.trim().split_whitespace().next()?; // "a/b/c/d"
    group.split('/').nth(1)?.trim().parse::<f64>().ok()
}

/// Spawn the background pinger. Every `ping_secs` it re-pings the union of
/// configured and observed IPs, so RTT stays current and IPs that appear later
/// on a port get discovered and pinged. Exits promptly when `exit` is set.
pub fn spawn(
    netmon: Arc<NetMon>,
    cfg: Config,
    registry: Arc<Registry>,
    exit: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("pinger".into())
        .spawn(move || {
            const COUNT: u32 = 3;
            const TIMEOUT_S: u32 = 1;
            let period = Duration::from_secs(if cfg.ping_secs == 0 { 30 } else { cfg.ping_secs });
            // Let the rx threads observe a few packets before the first round,
            // so port-only providers get pinged on cycle one rather than two.
            if sleep_interruptible(Duration::from_secs(2), &exit) {
                return;
            }
            loop {
                let observed = netmon.observed_snapshot();
                let mut targets: AHashSet<Ipv4Addr> = AHashSet::new();
                for p in &cfg.providers {
                    targets.extend(p.ips.iter().copied());
                }
                for ips in observed.values() {
                    targets.extend(ips.iter().copied());
                }
                for g in &cfg.grpc_sources {
                    if let Some((_, ip)) = grpc_target(&g.url) {
                        targets.insert(ip);
                    }
                }
                let _ = &registry; // reserved for future per-provider logging
                for ip in targets {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }
                    let rtt = ping_once(ip, COUNT, TIMEOUT_S);
                    netmon.record_ping(
                        ip,
                        PingSample {
                            rtt_ms: rtt,
                            checked_at_ns: now_unix_ns(),
                        },
                    );
                }
                if sleep_interruptible(period, &exit) {
                    return;
                }
            }
        })
        .expect("spawn pinger thread")
}

/// Sleep up to `dur`, waking every 200 ms to check `exit`. Returns true if
/// `exit` fired (caller should stop).
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
        // no scheme, no port
        assert_eq!(grpc_target("10.0.0.5").unwrap().0, "10.0.0.5");
        // https + path
        assert_eq!(grpc_target("https://1.2.3.4:443/foo").unwrap().0, "1.2.3.4");
    }

    #[test]
    fn no_summary_line_is_none() {
        assert_eq!(parse_avg_rtt("100% packet loss\n"), None);
    }
}
