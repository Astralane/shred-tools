use std::net::Ipv4Addr;

use ahash::AHashMap;

use crate::config::Config;

/// Provider index. Small and `Copy` so it rides the hot path for free.
pub type ProviderId = u16;

/// Resolves `(src_ip, dst_port)` to a provider, most-specific rule first:
///
///   1. `(ip, port)` — provider declared both
///   2. `port`       — provider declared only a port
///   3. `ip`         — provider declared only IPs
///
/// A packet that matches nothing is counted, not silently dropped.
pub struct Registry {
    names: Vec<String>,
    by_ip_port: AHashMap<(Ipv4Addr, u16), ProviderId>,
    by_port: AHashMap<u16, ProviderId>,
    by_ip: AHashMap<Ipv4Addr, ProviderId>,
}

impl Registry {
    pub fn build(cfg: &Config) -> Self {
        let mut names = Vec::with_capacity(cfg.providers.len());
        let mut by_ip_port = AHashMap::new();
        let mut by_port = AHashMap::new();
        let mut by_ip = AHashMap::new();

        for (idx, p) in cfg.providers.iter().enumerate() {
            let id = idx as ProviderId;
            names.push(p.name.clone());
            match (p.port, p.ips.is_empty()) {
                // both -> most specific
                (Some(port), false) => {
                    for ip in &p.ips {
                        by_ip_port.insert((*ip, port), id);
                    }
                }
                // port only
                (Some(port), true) => {
                    by_port.insert(port, id);
                }
                // ips only
                (None, false) => {
                    for ip in &p.ips {
                        by_ip.insert(*ip, id);
                    }
                }
                (None, true) => unreachable!("rejected by Config::validate"),
            }
        }

        Self {
            names,
            by_ip_port,
            by_port,
            by_ip,
        }
    }

    #[inline]
    pub fn resolve(&self, src_ip: Ipv4Addr, dst_port: u16) -> Option<ProviderId> {
        if let Some(id) = self.by_ip_port.get(&(src_ip, dst_port)) {
            return Some(*id);
        }
        if let Some(id) = self.by_port.get(&dst_port) {
            return Some(*id);
        }
        self.by_ip.get(&src_ip).copied()
    }

    pub fn name(&self, id: ProviderId) -> &str {
        &self.names[id as usize]
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderCfg;

    fn cfg(providers: Vec<ProviderCfg>) -> Config {
        Config {
            rpc_url: "http://x".into(),
            listen_ports: vec![1, 2, 3],
            bind_ip: Ipv4Addr::UNSPECIFIED,
            providers,
            output_dir: "./out".into(),
            rotate_secs: 600,
            verify_threads: 1,
            fec_max_wait_slots: 10,
            shred_version: None,
        }
    }

    #[test]
    fn ip_port_beats_port_beats_ip() {
        let a: Ipv4Addr = "10.0.0.1".parse().unwrap();
        let r = Registry::build(&cfg(vec![
            ProviderCfg { name: "both".into(), port: Some(1), ips: vec![a] },
            ProviderCfg { name: "port".into(), port: Some(1), ips: vec![] },
            ProviderCfg { name: "ip".into(), port: None, ips: vec![a] },
        ]));
        // exact (ip, port) wins
        assert_eq!(r.name(r.resolve(a, 1).unwrap()), "both");
        // port-only rule catches a different source IP on the same port
        assert_eq!(r.name(r.resolve("10.0.0.9".parse().unwrap(), 1).unwrap()), "port");
        // ip-only rule catches the same IP on a different port
        assert_eq!(r.name(r.resolve(a, 2).unwrap()), "ip");
        // nothing matches
        assert!(r.resolve("10.0.0.9".parse().unwrap(), 2).is_none());
    }

    #[test]
    fn port_pinned_to_unbound_port_is_rejected() {
        let mut c = cfg(vec![ProviderCfg { name: "x".into(), port: Some(9999), ips: vec![] }]);
        c.listen_ports = vec![1];
        assert!(c.validate().is_err());
    }
}
