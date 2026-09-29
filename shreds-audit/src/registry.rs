use std::net::Ipv4Addr;

use ahash::AHashMap;

use crate::config::Config;

pub type ProviderId = u16;

/// Resolves `(src_ip, dst_port)` to a provider: `(ip, port)`, then `port`, then `ip`.
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
                (Some(port), false) => {
                    for ip in &p.ips {
                        by_ip_port.insert((*ip, port), id);
                    }
                }
                (Some(port), true) => {
                    by_port.insert(port, id);
                }
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
        self.by_ip_port
            .get(&(src_ip, dst_port))
            .or_else(|| self.by_port.get(&dst_port))
            .or_else(|| self.by_ip.get(&src_ip))
            .copied()
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
            rpc_url: Some("http://x".into()),
            rpc: None,
            onchain_rpc: None,
            filter_rotation: Default::default(),
            listen_ports: vec![1, 2, 3],
            bind_ip: Ipv4Addr::UNSPECIFIED,
            providers,
            output_dir: "./out".into(),
            export: Default::default(),
            rotate_secs: 600,
            verify_threads: 1,
            fec_max_wait_slots: 10,
            shred_version: None,
            live_secs: 10,
            ping_secs: 30,
            grpc_sources: vec![],
            onchain_verify: true,
            onchain_sample_secs: 5,
            onchain_rpc_url: None,
            onchain_lag_slots: 32,
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
        assert_eq!(r.name(r.resolve(a, 1).unwrap()), "both");
        assert_eq!(r.name(r.resolve("10.0.0.9".parse().unwrap(), 1).unwrap()), "port");
        assert_eq!(r.name(r.resolve(a, 2).unwrap()), "ip");
        assert!(r.resolve("10.0.0.9".parse().unwrap(), 2).is_none());
    }

    #[test]
    fn port_pinned_to_unbound_port_is_rejected() {
        let mut c = cfg(vec![ProviderCfg { name: "x".into(), port: Some(9999), ips: vec![] }]);
        c.listen_ports = vec![1];
        assert!(c.validate().is_err());
    }
}
